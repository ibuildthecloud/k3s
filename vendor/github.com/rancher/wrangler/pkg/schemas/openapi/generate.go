package openapi

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	types "github.com/rancher/wrangler/pkg/schemas"
	"github.com/rancher/wrangler/pkg/schemas/definition"
	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
)

func MustGenerate(obj interface{}) *v1.JSONSchemaProps {
	if obj == nil {
		return nil
	}
	result, err := ToOpenAPIFromStruct(obj)
	if err != nil {
		panic(err)
	}
	return result
}

func ToOpenAPIFromStruct(obj interface{}) (*v1.JSONSchemaProps, error) {
	schemas := types.EmptySchemas()
	schema, err := schemas.Import(obj)
	if err != nil {
		return nil, err
	}

	return ToOpenAPI(schema.ID, schemas)
}

func ToOpenAPI(name string, schemas *types.Schemas) (*v1.JSONSchemaProps, error) {
	schema := schemas.Schema(name)
	if schema == nil {
		return nil, fmt.Errorf("failed to find schema: %s", name)
	}

	newSchema := schema.DeepCopy()
	if newSchema.InternalSchema != nil {
		newSchema = newSchema.InternalSchema.DeepCopy()
	}
	delete(newSchema.ResourceFields, "kind")
	delete(newSchema.ResourceFields, "apiVersion")
	delete(newSchema.ResourceFields, "metadata")

	return schemaToProps(newSchema, schemas, map[string]bool{})
}

func populateField(fieldJSP *v1.JSONSchemaProps, f *types.Field) error {
	fieldJSP.Description = f.Description
	// don't reset this to not nullable
	if f.Nullable {
		fieldJSP.Nullable = f.Nullable
	}
	fieldJSP.MinLength = f.MinLength
	fieldJSP.MaxLength = f.MaxLength

	if f.Type == "string" && len(f.Options) > 0 {
		for _, opt := range append(f.Options, "") {
			bytes, err := json.Marshal(&opt)
			if err != nil {
				return err
			}
			fieldJSP.Enum = append(fieldJSP.Enum, v1.JSON{
				Raw: bytes,
			})
		}
	}

	if len(f.InvalidChars) > 0 {
		fieldJSP.Pattern = fmt.Sprintf("^[^%s]*$", f.InvalidChars)
	}

	if len(f.ValidChars) > 0 {
		fieldJSP.Pattern = fmt.Sprintf("^[%s]*$", f.ValidChars)
	}

	if f.Min != nil {
		fl := float64(*f.Min)
		fieldJSP.Minimum = &fl
	}

	if f.Max != nil {
		fl := float64(*f.Max)
		fieldJSP.Maximum = &fl
	}

	if f.Default != nil {
		bytes, err := json.Marshal(f.Default)
		if err != nil {
			return err
		}
		fieldJSP.Default = &v1.JSON{
			Raw: bytes,
		}
	}

	return nil
}

func typeToProps(typeName string, schemas *types.Schemas, inflight map[string]bool) (*v1.JSONSchemaProps, error) {
	t, subType, schema, err := typeAndSchema(typeName, schemas)
	if err != nil {
		return nil, err
	}

	if schema != nil {
		return schemaToProps(schema, schemas, inflight)
	}

	jsp := &v1.JSONSchemaProps{}

	switch t {
	case "map":
		additionalProps, err := typeToProps(subType, schemas, inflight)
		if err != nil {
			return nil, err
		}
		jsp.Type = "object"
		jsp.Nullable = true
		if subType != "json" {
			jsp.AdditionalProperties = &v1.JSONSchemaPropsOrBool{
				Allows: true,
				Schema: additionalProps,
			}
		}
	case "array":
		items, err := typeToProps(subType, schemas, inflight)
		if err != nil {
			return nil, err
		}
		jsp.Type = "array"
		jsp.Nullable = true
		jsp.Items = &v1.JSONSchemaPropsOrArray{
			Schema: items,
		}
	case "string":
		jsp.Type = t
		jsp.Nullable = true
	default:
		jsp.Type = t
	}

	if jsp.Type == "object" && jsp.AdditionalProperties == nil {
		jsp.XPreserveUnknownFields = &[]bool{true}[0]
	}

	return jsp, nil
}

func schemaToProps(schema *types.Schema, schemas *types.Schemas, inflight map[string]bool) (*v1.JSONSchemaProps, error) {
	jsp := &v1.JSONSchemaProps{
		Description: schema.Description,
		Type:        "object",
	}

	if inflight[schema.ID] {
		return jsp, nil
	}

	inflight[schema.ID] = true
	defer delete(inflight, schema.ID)

	jsp.Properties = map[string]v1.JSONSchemaProps{}

	for name, f := range schema.ResourceFields {
		fieldJSP, err := typeToProps(f.Type, schemas, inflight)
		if err != nil {
			return nil, err
		}
		if err := populateField(fieldJSP, &f); err != nil {
			return nil, err
		}
		if f.Required {
			jsp.Required = append(jsp.Required, name)
		}
		jsp.Properties[name] = *fieldJSP
	}

	sort.Strings(jsp.Required)
	if len(jsp.Properties) == 0 && strings.HasSuffix(strings.ToLower(schema.ID), "map") {
		jsp.XPreserveUnknownFields = &[]bool{true}[0]
	}
	return jsp, nil
}

func typeAndSchema(typeName string, schemas *types.Schemas) (string, string, *types.Schema, error) {
	if definition.IsReferenceType(typeName) {
		return "string", "", nil, nil
	}

	if definition.IsArrayType(typeName) {
		return "array", definition.SubType(typeName), nil, nil
	}

	if definition.IsMapType(typeName) {
		return "map", definition.SubType(typeName), nil, nil
	}

	switch typeName {
	// TODO: in v1 set the x- header for this
	case "intOrString":
		return "string", "", nil, nil
	case "int":
		return "integer", "", nil, nil
	case "float":
		return "number", "", nil, nil
	case "string":
		return "string", "", nil, nil
	case "date":
		return "string", "", nil, nil
	case "enum":
		return "string", "", nil, nil
	case "base64":
		return "string", "", nil, nil
	case "password":
		return "string", "", nil, nil
	case "hostname":
		return "string", "", nil, nil
	case "boolean":
		return "boolean", "", nil, nil
	case "json":
		return "object", "", nil, nil
	}

	schema := schemas.Schema(typeName)
	if schema == nil {
		return "", "", nil, fmt.Errorf("failed to find schema %s", typeName)
	}
	if schema.InternalSchema != nil {
		return "", "", schema.InternalSchema, nil
	}
	return "", "", schema, nil
}
