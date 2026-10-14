package file

import (
	"os"

	"gopkg.in/yaml.v3"
)

func ReadFileFromYaml(pathFile string, target any) error {
	yamlFile, err := os.ReadFile(pathFile)
	if err != nil {
		return err
	}

	return yaml.Unmarshal(yamlFile, target)
}
