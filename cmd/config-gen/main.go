package main

import (
	"bytes"
	_ "embed"
	"encoding/base64"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"gopkg.in/yaml.v3"
)

//go:embed config-template-full.yaml
var templateContent string

// IntegrationConfigs represents the input configuration structure
type IntegrationConfigs struct {
	Integrations []IntegrationConfig `yaml:"integrations"`
}

// IntegrationConfig represents the input configuration structure
type IntegrationConfig struct {
	IntegrationID   string `yaml:"integrationId"`
	IntegrationName string `yaml:"integrationName"`
	ModuleName      string `yaml:"moduleName"`
	ModuleConfig    string `yaml:"moduleConfig"`
}

func main() {
	// Define command-line flags for input and output directories
	inputFile := flag.String("input", "input-config.yaml", "Path to the input JSON/YAML config file")
	outputDir := flag.String("output", "output-configs/", "Directory to write the generated YAML files")
	flag.Parse()

	// Load input configuration
	configs, err := loadConfig(*inputFile)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Parse the template
	tmpl, err := template.New("yamlTemplate").Funcs(
		template.FuncMap{
			"base64": func(s string) string {
				return base64.StdEncoding.EncodeToString([]byte(s))
			},
			"toLower": strings.ToLower,
		}).Parse(templateContent)
	if err != nil {
		log.Fatalf("Failed to parse template: %v", err)
	}

	// Ensure output directory exists
	err = os.MkdirAll(*outputDir, 0750)
	if err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}

	// Generate YAML files for each integration config
	err = generateYAMLFile(tmpl, *configs, *outputDir)
	if err != nil {
		log.Printf("Failed to generate ingest YAML file: %v", err)
	} else {
		log.Printf("Successfully generated ingeset YAML file!")
	}
}

// loadConfig loads the list of integration configurations from a YAML/JSON file.
func loadConfig(filePath string) (*IntegrationConfigs, error) {
	data, err := os.ReadFile(filepath.Clean(filePath))
	if err != nil {
		return nil, fmt.Errorf("error reading config file: %w", err)
	}

	var configs IntegrationConfigs
	err = yaml.Unmarshal(data, &configs)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling config: %w", err)
	}

	return &configs, nil
}

// generateYAMLFile generates a YAML file for a given integration config using the provided template.
func generateYAMLFile(tmpl *template.Template, config IntegrationConfigs, outputDir string) error {
	// Execute the template with the integration data
	var buffer bytes.Buffer
	err := tmpl.Execute(&buffer, config)
	if err != nil {
		return fmt.Errorf("error executing template: %w", err)
	}

	// Write the output to a YAML file in the output directory
	outputFile := filepath.Join(outputDir, "external-ingest.yaml")
	err = os.WriteFile(outputFile, buffer.Bytes(), 0644)
	if err != nil {
		return fmt.Errorf("error writing YAML file: %w", err)
	}

	return nil
}
