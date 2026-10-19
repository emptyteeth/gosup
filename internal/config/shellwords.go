package config

import (
	"fmt"
	"strings"
)

// splitCommand splits command into argv.
//
// Rules (shell-like, but without variable/glob expansion):
//   - whitespace separates arguments
//   - content inside single quotes is kept verbatim
//   - inside double quotes, \" and \\ escapes are allowed
//   - outside quotes, \ escapes the next character
//
// If you do not want this splitting (for example when you need ";" or "|"), set shell = true and let /bin/sh -c handle it.
func splitCommand(s string) ([]string, error) {
	var argv []string
	var cur strings.Builder
	started := false // whether an argument has been entered (to support empty arguments "")
	inSingle, inDouble := false, false

	flush := func() {
		if started {
			argv = append(argv, cur.String())
			cur.Reset()
			started = false
		}
	}

	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case inSingle:
			if c == '\'' {
				inSingle = false
			} else {
				cur.WriteByte(c)
			}
		case inDouble:
			switch c {
			case '"':
				inDouble = false
			case '\\':
				if i+1 >= len(s) {
					return nil, fmt.Errorf("backslash inside double quotes is missing a following character")
				}
				i++
				switch s[i] {
				case '"', '\\', '$', '`':
					cur.WriteByte(s[i])
				default:
					// For other characters keep the backslash, matching POSIX sh behavior inside double quotes.
					cur.WriteByte('\\')
					cur.WriteByte(s[i])
				}
			default:
				cur.WriteByte(c)
			}
		default:
			switch c {
			case '\'':
				inSingle = true
				started = true
			case '"':
				inDouble = true
				started = true
			case '\\':
				if i+1 >= len(s) {
					return nil, fmt.Errorf("trailing backslash has no character to escape")
				}
				i++
				cur.WriteByte(s[i])
				started = true
			case ' ', '\t', '\n', '\r':
				flush()
			default:
				cur.WriteByte(c)
				started = true
			}
		}
	}
	if inSingle {
		return nil, fmt.Errorf("unclosed single quote")
	}
	if inDouble {
		return nil, fmt.Errorf("unclosed double quote")
	}
	flush()
	return argv, nil
}

// SplitCommand exports splitCommand for tests and documentation examples.
func SplitCommand(s string) ([]string, error) { return splitCommand(s) }
