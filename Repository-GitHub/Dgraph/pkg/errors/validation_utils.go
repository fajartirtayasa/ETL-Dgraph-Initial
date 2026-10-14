package errors

// ConvertValidationErrorsToMap converts ValidationErrors to a map[string]string format
// for consistent API error responses across request validation and usecase validation
func ConvertValidationErrorsToMap(vErr *ValidationErrors) map[string]string {
	errMap := make(map[string]string)
	for _, validationErr := range vErr.Errors {
		errMap[validationErr.Field] = validationErr.Message
	}
	return errMap
}
