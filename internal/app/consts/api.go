package consts

import "net/http"

// API Statuses
const (
	ApiStatusSuccess = "success"
	ApiStatusError   = "error"
)

const (
	// General Errors (5xx)
	CodeInternalServerError = "INTERNAL_SERVER_ERROR"
	CodeServiceUnavailable  = "SERVICE_UNAVAILABLE"
	CodeUnexpectedError     = "UNEXPECTED_ERROR"

	// Request Errors (4xx)
	CodeBadRequest        = "BAD_REQUEST"
	CodeValidationFailure = "VALIDATION_FAILURE"
	CodeNotFound          = "NOT_FOUND"
	CodeMethodNotAllowed  = "METHOD_NOT_ALLOWED"

	// Authentication & Authorization Errors (4xx)
	CodeUnauthorized       = "UNAUTHORIZED"
	CodeForbidden          = "FORBIDDEN"
	CodeInvalidCredentials = "INVALID_CREDENTIALS"
	CodeTokenExpired       = "TOKEN_EXPIRED"
	CodeTokenInvalid       = "TOKEN_INVALID"
)

// --- Standard Error Messages ---
const (
	// General Errors
	MessageInternalServerError = "Maaf, terjadi kesalahan tak terduga. Mohon coba lagi sebentar lagi."
	MessageServiceUnavailable  = "Maaf, layanan sedang dalam pemeliharaan. Silakan coba lagi sebentar lagi."
	MessageUnexpectedError     = "Maaf, kami sedang mengalami kesulitan teknis. Tim kami sedang menanganinya."

	// Request Errors
	MessageBadRequest        = "Maaf, permintaan Anda tidak dapat diproses. Mohon periksa kembali data yang Anda masukkan."
	MessageValidationFailure = "Data yang Anda masukkan tidak valid. Silakan periksa kembali data Anda."
	MessageNotFound          = "Maaf, halaman atau data yang Anda cari tidak ditemukan."
	MessageMethodNotAllowed  = "Maaf, tindakan ini tidak diizinkan."

	// Auth Errors
	MessageUnauthorized       = "Anda perlu masuk untuk mengakses konten ini."
	MessageForbidden          = "Anda tidak memiliki izin untuk mengakses fitur ini."
	MessageInvalidCredentials = "Identitas atau kata sandi yang Anda masukkan salah."
	MessageTokenExpired       = "Sesi Anda telah berakhir. Silakan masuk kembali."
	MessageTokenInvalid       = "Maaf, ada masalah dengan sesi Anda. Silakan masuk kembali."
)

// --- Mappings ---

// ErrorCodeToHTTPStatus maps our internal API error codes to the appropriate HTTP status codes.
var ErrorCodeToHTTPStatus = map[string]int{
	// 5xx Server Errors
	CodeInternalServerError: http.StatusInternalServerError,
	CodeServiceUnavailable:  http.StatusServiceUnavailable,
	CodeUnexpectedError:     http.StatusInternalServerError,

	// 4xx Client Errors
	CodeBadRequest:        http.StatusBadRequest,
	CodeValidationFailure: http.StatusBadRequest,
	CodeNotFound:          http.StatusNotFound,
	CodeMethodNotAllowed:  http.StatusMethodNotAllowed,

	// 4xx Auth Errors
	CodeUnauthorized:       http.StatusUnauthorized,
	CodeForbidden:          http.StatusForbidden,
	CodeInvalidCredentials: http.StatusUnauthorized,
	CodeTokenExpired:       http.StatusUnauthorized,
	CodeTokenInvalid:       http.StatusUnauthorized,
}

// ErrorCodeToMessage maps our internal API error codes to their default human-readable messages.
var ErrorCodeToMessage = map[string]string{
	// 5xx Server Errors
	CodeInternalServerError: MessageInternalServerError,
	CodeServiceUnavailable:  MessageServiceUnavailable,
	CodeUnexpectedError:     MessageUnexpectedError,

	// 4xx Client Errors
	CodeBadRequest:        MessageBadRequest,
	CodeValidationFailure: MessageValidationFailure,
	CodeNotFound:          MessageNotFound,
	CodeMethodNotAllowed:  MessageMethodNotAllowed,

	// 4xx Auth Errors
	CodeUnauthorized:       MessageUnauthorized,
	CodeForbidden:          MessageForbidden,
	CodeInvalidCredentials: MessageInvalidCredentials,
	CodeTokenExpired:       MessageTokenExpired,
	CodeTokenInvalid:       MessageTokenInvalid,
}

const (
	DefaultPaginationLimit  int = 10
	DefaultPaginationOffset int = 0
)
