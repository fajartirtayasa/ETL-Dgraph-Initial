package errors

// ValidationError represents a single field validation error.
type ValidationError struct {
    Field   string `json:"field"`
    Message string `json:"message"`
}

func (e *ValidationError) Error() string {
    return e.Message
}

// ValidationErrors aggregates multiple validation errors.
type ValidationErrors struct {
    Errors []ValidationError `json:"errors"`
}

func (v *ValidationErrors) Error() string {
    if len(v.Errors) == 0 {
        return "validation error"
    }
    return v.Errors[0].Message
}

func NewValidationError(field, message string) *ValidationErrors {
    return &ValidationErrors{Errors: []ValidationError{{Field: field, Message: message}}}
}
