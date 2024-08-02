package handler

import (
	"context"
	"fmt"
	"image/color"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/llms/ollama"
	"github.com/xo/resvg"
	"github.com/xo/usql/env"
	"github.com/xo/usql/metacmd"
)

const gnuplotHint = "--!gnuplot"
const text2sqlHint = "--!text2sql"
const sqlHint = "--!sql"

func moShouldHijack(qstr string) bool {
	return strings.HasPrefix(qstr, gnuplotHint) || strings.HasPrefix(qstr, text2sqlHint)
}

func splitQstr(qstr string) (string, string, string) {
	gnuplotBuf := strings.Builder{}
	text2sqlBuf := strings.Builder{}
	sqlBuf := strings.Builder{}

	// write buffer, initially nil
	var wbuf *strings.Builder

	lines := strings.Split(qstr, "\n")
	for _, line := range lines {
		// once we start writing sql, never go back.
		if wbuf != &sqlBuf {
			// mode switch
			if strings.HasPrefix(line, gnuplotHint) {
				wbuf = &gnuplotBuf
			} else if strings.HasPrefix(line, text2sqlHint) {
				wbuf = &text2sqlBuf
				idx := strings.IndexAny(line, " \t")
				if idx > 0 {
					rest := strings.TrimSpace(line[idx:])
					if rest != "" {
						qline := lookupQueryDesc(rest)
						wbuf.WriteString(qline)
						wbuf.WriteString("\n")
						line = ""
					}
				}
			} else if !strings.HasPrefix(line, "--") || strings.HasPrefix(line, sqlHint) {
				wbuf = &sqlBuf
			}
			if wbuf == nil {
				// fallback to sql mode, should not happen because we hijacked, but anyway.
				wbuf = &sqlBuf
			}
		}

		if wbuf == &sqlBuf {
			wbuf.WriteString(line)
			wbuf.WriteString("\n")
		} else {
			idx := strings.IndexAny(line, " \t")
			if idx > 0 {
				wbuf.WriteString(line[idx:])
			}
			wbuf.WriteString("\n")
		}
	}
	return gnuplotBuf.String(), text2sqlBuf.String(), sqlBuf.String()
}

func moHijack(h *Handler, ctx context.Context, w io.Writer, opt metacmd.Option, prefix, qstr string, qtyp bool, bind []any) error {
	var err error
	stdout := h.IO().Stdout()
	gnuplotStr, text2sqlStr, sqlStr := splitQstr(qstr)

	// trim spaces
	sqlStr = strings.TrimSpace(sqlStr)

	// if we have text2sqlStr and not having sqlStr, ask LLM to help.
	fmt.Fprintln(stdout, "========= MO HIJACK =========")
	fmt.Fprintln(stdout, "gnuplotStr: ", gnuplotStr)
	fmt.Fprintln(stdout, "text2sqlStr: ", text2sqlStr)
	fmt.Fprintln(stdout, "sqlStr: ", sqlStr)
	fmt.Fprintln(stdout, "========= MO HIJACK =========")

	if text2sqlStr != "" && sqlStr == ";" {
		var tmpSqlStr string
		if tmpSqlStr, err = text2sql(h, ctx, text2sqlStr); err != nil {
			return err
		}
		fmt.Fprintln(stdout, "========= Prompting LLM =========")
		fmt.Fprintln(h.IO().Stdout(), tmpSqlStr)
		fmt.Fprintln(stdout, "========= Prompting LLM =========")
		sqlStr = tmpSqlStr + ";"
	}

	if gnuplotStr == "" {
		fmt.Fprintln(stdout, "========= Executing SQL =========")
		fmt.Fprintln(stdout, "prefix: ", prefix)
		fmt.Fprintln(stdout, "sqlStr: ", sqlStr)
		return h.doExecSingle(ctx, w, opt, prefix, sqlStr, true, bind)
	} else {
		fmt.Fprintln(stdout, "========= Executing gnuplotStr =========")
		return createGnuplotFile(h, ctx, sqlStr, gnuplotStr, bind)
	}
}

func createGnuplotFile(h *Handler, ctx context.Context, sqlStr, gnuplotStr string, bind []any) error {
	tmpDir, ok := os.LookupEnv("MO_TMPDIR")
	if !ok {
		tmpDir = "/tmp"
	}

	gnuplotCmd, ok := os.LookupEnv("MO_GNUPLOT")
	if !ok {
		gnuplotCmd = "/opt/homebrew/bin/gnuplot"
	}

	plotFn := fmt.Sprintf("%s/mo_usql_gnuplot.plot", tmpDir)
	svgFn := fmt.Sprintf("%s/mo_usql_gnuplot.svg", tmpDir)

	rows, err := h.DB().QueryContext(ctx, sqlStr, bind...)
	if err != nil {
		return err
	}
	defer rows.Close()

	plotFile, err := os.Create(plotFn)
	if err != nil {
		return err
	}
	defer plotFile.Close()

	// Write query result as inline data.
	plotFile.WriteString("$DATA << EOD\n")
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	ncol := len(cols)
	tfmt := env.GoTime()
	for rows.Next() {
		row, err := h.scan(rows, ncol, tfmt)
		if err != nil {
			return err
		}
		for i, v := range row {
			// replace newlines with __
			vv := strings.ReplaceAll(v, "\r\n", "__")
			vv = strings.ReplaceAll(vv, "\n", "__")
			// replace " with '
			vv = strings.ReplaceAll(vv, "\"", "'")
			// replace tabs and spaces with _
			vv = strings.ReplaceAll(vv, "\t", "_")
			vv = strings.ReplaceAll(vv, " ", "_")

			plotFile.WriteString(vv)
			if i < ncol-1 {
				plotFile.WriteString(" ")
			} else {
				plotFile.WriteString("\n")
			}
		}
	}
	plotFile.WriteString("EOD\n")

	// Write gnuplot script. First write some defaults.  User can override.
	// but overwrite output file name is almost always a bad idea.
	plotFile.WriteString("set term svg size 800,600\n")
	plotFile.WriteString("set output '" + svgFn + "'\n")
	plotFile.WriteString("set boxwidth 0.5\n")
	plotFile.WriteString(gnuplotStr)

	cmd := exec.Command(gnuplotCmd, plotFn)
	_, err = cmd.Output()
	if err != nil {
		return err
	}
	svg, err := os.ReadFile(svgFn)
	if err != nil {
		return err
	}
	// bg white: gnuplot is usually tuned with white background.
	img, err := resvg.Render(svg, resvg.WithBackground(color.White))
	if err != nil {
		return err
	}

	typ := env.TermGraphics()
	if !typ.Available() {
		return fmt.Errorf("graphics not available")
	}

	return typ.Encode(h.IO().Stdout(), img)
}

func queryOneStringValue(h *Handler, ctx context.Context, skip1 bool, query string) (string, error) {
	var err error
	rows, err := h.DB().QueryContext(ctx, query)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	if rows.Next() {
		var val string
		if skip1 {
			var tmp string
			err = rows.Scan(&tmp, &val)
		} else {
			err = rows.Scan(&val)
		}
		if err != nil {
			return "", err
		}
		return val, nil
	}
	return "", nil
}

func queryManyStringValue(h *Handler, ctx context.Context, query string) ([]string, error) {
	ret := []string{}
	rows, err := h.DB().QueryContext(ctx, query)
	if err != nil {
		return ret, err
	}
	defer rows.Close()
	for rows.Next() {
		var val string
		err := rows.Scan(&val)
		if err != nil {
			return ret, err
		}
		ret = append(ret, val)
	}
	return ret, nil
}

// The art of computer programming is to ask LLM "nicely".
// The following prompt is from sqlcoder.
const promptHeader string = `
### Instructions:
Your task is to convert a question into a SQL query, given a MySQL database schema.
Adhere to these rules:
- **Deliberately go through the question and database schema word by word** to appropriately answer the question
- **Use Table Aliases** to prevent ambiguity. For example,` + " `SELECT table1.col1, table2.col1 FROM table1 JOIN table2 ON table1.id = table2.id`." + `
- When creating a ratio, always cast the numerator as float

### Input:
Generate a SQL query that answers the question ` + "`%s`." + `
This query will run on a database whose schema is represented in this string:
`

const promptFooter string = `
### Response:
Based on your instructions, here is the SQL query I have generated to answer the question ` + "`%s`:\n```sql\n"

// global variables, yeah, I know it is bad.
var moDbName string
var moTableSchemaStr string

func text2sql(h *Handler, ctx context.Context, text2sqlStr string) (string, error) {
	var err error
	if moDbName == "" {
		moDbName, err = queryOneStringValue(h, ctx, false, "select database()")
		if err != nil {
			return "", err
		}

		tables, err := queryManyStringValue(h, ctx, "show tables")
		if err != nil {
			return "", err
		}

		buf := strings.Builder{}
		for _, table := range tables {
			schema, err := queryOneStringValue(h, ctx, true, fmt.Sprintf("show create table %s", table))
			if err != nil {
				return "", err
			}
			buf.WriteString(schema)
			buf.WriteString("\n")
		}
		moTableSchemaStr = buf.String()
	}

	buf := strings.Builder{}
	buf.WriteString(fmt.Sprintf(promptHeader, text2sqlStr))
	buf.WriteString(moTableSchemaStr)
	buf.WriteString(fmt.Sprintf(promptFooter, text2sqlStr))

	model, ok := os.LookupEnv("MO_LLM_MODEL")
	if !ok {
		model = "llama3.1"
	}

	// prompt llm
	llm, err := ollama.New(ollama.WithModel(model))
	if err != nil {
		return "", err
	}
	prompt := []llms.MessageContent{
		llms.TextParts(llms.ChatMessageTypeSystem, "You are a SQL/Database expert that helps user to convert a question into a SQL query."),
		llms.TextParts(llms.ChatMessageTypeHuman, buf.String()),
	}
	completion, err := llm.GenerateContent(ctx, prompt, llms.WithTemperature(0.1))
	if err != nil {
		return "", err
	}
	if len(completion.Choices) == 0 {
		return "", fmt.Errorf("no completion")
	}

	lines := strings.Split(completion.Choices[0].Content, "\n")
	sqlBuf := &strings.Builder{}
	for _, line := range lines {
		if strings.HasPrefix(line, "```sql") {
			sqlBuf.Reset()
			continue
		} else if strings.HasPrefix(line, "```") {
			// done
			break
		}
		sqlBuf.WriteString(line)
		sqlBuf.WriteString("\n")
	}

	return sqlBuf.String(), nil
}

func lookupQueryDesc(q string) string {
	switch q {
	case "tpch-q1":
		return `
List return flag, line status, 
totals of extended price, discounted extended price,
discounted extended price plus tax, average quantity, 
average extended price and average discount for all orders 
whose ship date is between 90 days before 1998-12-01 and 
1998-12-01.  Group result by return flag and line status,
sorted by return flag and line status in ascending order. 
`
	default:
		return ""
	}
}
