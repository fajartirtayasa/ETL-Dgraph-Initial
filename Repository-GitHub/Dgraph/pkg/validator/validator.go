package validator

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/go-playground/validator/v10"
)

type Validator struct {
	validate *validator.Validate
}

// New creates a new Validator instance.
func NewValidator() *Validator {
	v := validator.New()

	return &Validator{
		validate: v,
	}
}

// Validate performs validation on a struct and returns a map of user-friendly errors.
func (v *Validator) Validate(s any) map[string]string {
	errorMessages := make(map[string]string)

	err := v.validate.Struct(s)
	if err != nil {
		for _, err := range err.(validator.ValidationErrors) {
			fieldName := err.Field()

			// Get the type of the struct
			t := reflect.TypeOf(s)
			if t.Kind() == reflect.Ptr {
				t = t.Elem()
			}

			if field, ok := t.FieldByName(fieldName); ok {
				jsonTag := field.Tag.Get("json")
				if jsonTag != "" && jsonTag != "-" {
					fieldName = jsonTag
				}
			}

			errorMessages[fieldName] = getErrorMessage(err, fieldName)
		}
	}

	if len(errorMessages) > 0 {
		return errorMessages
	}
	return nil
}

// getErrorMessage returns a user-friendly message in Bahasa Indonesia for a given validation error.
func getErrorMessage(err validator.FieldError, fieldName string) string {
	tag := err.Tag()
	param := err.Param()

	// Convert field name to more user-friendly Indonesian terms
	indonesianFieldName := getIndonesianFieldName(fieldName)

	switch tag {
	// --- General Validations ---
	case "required":
		return fmt.Sprintf("%s wajib diisi.", indonesianFieldName)
	case "boolean":
		return fmt.Sprintf("%s harus berupa nilai boolean yang valid (true atau false).", indonesianFieldName)
	case "oneof":
		return fmt.Sprintf("%s harus salah satu dari: %s.", indonesianFieldName, strings.ReplaceAll(param, " ", ", "))
	case "len":
		return fmt.Sprintf("%s harus tepat %s karakter.", indonesianFieldName, param)

	// --- String Validations ---
	case "min":
		return fmt.Sprintf("%s minimal %s karakter.", indonesianFieldName, param)
	case "max":
		return fmt.Sprintf("%s maksimal %s karakter.", indonesianFieldName, param)
	case "alpha":
		return fmt.Sprintf("%s hanya boleh berisi huruf.", indonesianFieldName)
	case "alphanum":
		return fmt.Sprintf("%s hanya boleh berisi huruf dan angka.", indonesianFieldName)
	case "uuid":
		return fmt.Sprintf("%s harus berupa UUID yang valid.", indonesianFieldName)
	case "contains":
		return fmt.Sprintf("%s harus berisi teks '%s'.", indonesianFieldName, param)

	// --- Numeric Validations ---
	case "numeric":
		return fmt.Sprintf("%s harus berupa nilai numerik.", indonesianFieldName)
	case "gte":
		return fmt.Sprintf("%s harus %s atau lebih besar.", indonesianFieldName, param)
	case "gt":
		return fmt.Sprintf("%s harus lebih besar dari %s.", indonesianFieldName, param)
	case "lte":
		return fmt.Sprintf("%s harus %s atau lebih kecil.", indonesianFieldName, param)
	case "lt":
		return fmt.Sprintf("%s harus lebih kecil dari %s.", indonesianFieldName, param)
	case "eq":
		return fmt.Sprintf("%s harus sama dengan %s.", indonesianFieldName, param)
	case "ne":
		return fmt.Sprintf("%s tidak boleh sama dengan %s.", indonesianFieldName, param)

	// --- Network & Format Validations ---
	case "email":
		return fmt.Sprintf("%s harus berupa alamat email yang valid.", indonesianFieldName)
	case "url":
		return fmt.Sprintf("%s harus berupa URL yang valid.", indonesianFieldName)
	case "uri":
		return fmt.Sprintf("%s harus berupa URI yang valid.", indonesianFieldName)
	case "ip":
		return fmt.Sprintf("%s harus berupa alamat IP yang valid.", indonesianFieldName)
	case "ipv4":
		return fmt.Sprintf("%s harus berupa alamat IPv4 yang valid.", indonesianFieldName)
	case "ipv6":
		return fmt.Sprintf("%s harus berupa alamat IPv6 yang valid.", indonesianFieldName)
	case "cidr":
		return fmt.Sprintf("%s harus berupa notasi CIDR yang valid.", indonesianFieldName)
	case "mac":
		return fmt.Sprintf("%s harus berupa alamat MAC yang valid.", indonesianFieldName)
	case "hostname":
		return fmt.Sprintf("%s harus berupa hostname yang valid.", indonesianFieldName)
	case "credit_card":
		return fmt.Sprintf("%s harus berupa nomor kartu kredit yang valid.", indonesianFieldName)
	case "json":
		return fmt.Sprintf("%s harus berupa string JSON yang valid.", indonesianFieldName)
	case "base64":
		return fmt.Sprintf("%s harus berupa string Base64 yang valid.", indonesianFieldName)
	case "identity_number":
		return fmt.Sprintf("%s harus sejumlah 8 (Polri) / 18 (PNS Polri) digit.", indonesianFieldName)

	default:
		// Fallback for any other validation tags
		return fmt.Sprintf("Field %s tidak valid.", indonesianFieldName)
	}
}

// getIndonesianFieldName converts field names to user-friendly Indonesian terms
func getIndonesianFieldName(field string) string {
	fieldMap := map[string]string{
		// User related fields
		"name":             "Nama",
		"email":            "Email",
		"password":         "Password",
		"confirm_password": "Konfirmasi Password",
		"new_password":     "Password Baru",
		"phone_number":     "Nomor Telepon",
		"identity_number":  "Nomor Identitas",
		"gender":           "Jenis Kelamin",
		"is_headscarf":     "Jilbab",
		"is_active":        "Status Aktif",

		// Profile related fields
		"institute":          "Lembaga",
		"personnel_category": "Kategori Personel",
		"satker_id":          "ID Satker",
		"grade_id":           "ID Pangkat",
		"position":           "Jabatan",
		"status":             "Status",

		// Clothing related fields
		"clothing_type_id": "ID Jenis Pakaian",
		"size":             "Ukuran",
		"is_custom":        "Ukuran Kustom",
		"is_changed":       "Perubahan Ukuran",

		// General fields
		"id":         "ID",
		"created_at": "Tanggal Dibuat",
		"updated_at": "Tanggal Diperbarui",
		"deleted_at": "Tanggal Dihapus",
	}

	if indonesianName, exists := fieldMap[field]; exists {
		return indonesianName
	}

	// If no mapping found, return the original field name
	return field
}
