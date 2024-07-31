package handler

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

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
	stdout := h.IO().Stdout()

	gnuplotStr, text2sqlStr, sqlStr := splitQstr(qstr)

	// NYI.  Just do a debug print for now.
	fmt.Fprintln(stdout, "=== MO HIJACK ===")
	fmt.Fprintln(stdout, "opt:", opt)
	fmt.Fprintln(stdout, "prefix:", prefix)
	fmt.Fprintln(stdout, "qstr:", qstr)
	fmt.Fprintln(stdout, "qtyp:", qtyp)
	fmt.Fprintln(stdout, "bind:", bind)
	fmt.Fprintln(stdout, "------")
	fmt.Fprintln(stdout, "gnuplotStr:", gnuplotStr)
	fmt.Fprintln(stdout, "text2sqlStr:", text2sqlStr)
	fmt.Fprintln(stdout, "sqlStr:", sqlStr)
	fmt.Fprintln(stdout, "=== MO HIJACK ===")

	// if we have text2sqlStr and not having sqlStr, ask LLM to help.
	if text2sqlStr != "" && sqlStr != "" {
		// NYI.
	}

	// no sql, nothing to do.
	if sqlStr == "" {
		return nil
	}

	if gnuplotStr == "" {
		return h.doExecSingle(ctx, w, opt, prefix, sqlStr, qtyp, bind)
	} else {
		return createGnuplotFile(h, ctx, sqlStr, gnuplotStr, bind)
	}
}

func createGnuplotFile(h *Handler, ctx context.Context, sqlStr, gnuplotStr string, bind []any) error {
	tmpDir, ok := os.LookupEnv("MO_TMPDIR")
	if !ok {
		tmpDir = "/tmp"
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
	plotFile.WriteString(gnuplotStr)

	cmd := exec.Command("gnuplot", plotFn)
	_, err = cmd.Output()
	if err != nil {
		return err
	}
	svg, err := os.ReadFile(svgFn)
	if err != nil {
		return err
	}
	img, err := resvg.Render(svg)
	if err != nil {
		return err
	}

	typ := env.TermGraphics()
	if !typ.Available() {
		return fmt.Errorf("graphics not available")
	}

	return typ.Encode(h.IO().Stdout(), img)
}
