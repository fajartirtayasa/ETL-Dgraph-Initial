package common

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	// XSDInt represents the XML schema type for integers.
	XSDInt = "http://www.w3.org/2001/XMLSchema#int"
	// XSDFloat represents the XML schema type for floats.
	XSDFloat = "http://www.w3.org/2001/XMLSchema#float"
)

// StringLiteral escapes the supplied value as an RDF string literal.
func StringLiteral(value string) string {
	return strconv.Quote(value)
}

// IntLiteral converts an int64 into an RDF typed literal.
func IntLiteral(value int64) string {
	return fmt.Sprintf("\"%d\"^^<%s>", value, XSDInt)
}

// FloatLiteral converts a float64 into an RDF typed literal. It trims trailing
// zeros for readability while preserving precision.
func FloatLiteral(value float64) string {
	s := strconv.FormatFloat(value, 'f', -1, 64)
	return fmt.Sprintf("\"%s\"^^<%s>", s, XSDFloat)
}

// AppendTriple appends a single RDF triple to the given builder in N-Quad
// format and terminates it with a newline.
func AppendTriple(builder *strings.Builder, subject, predicate, object string) {
	builder.WriteString(subject)
	builder.WriteByte(' ')
	builder.WriteString(predicate)
	builder.WriteByte(' ')
	builder.WriteString(object)
	builder.WriteString(" .\n")
}
