package schemas

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/rancher/wrangler/pkg/data/convert"
	"github.com/rancher/wrangler/pkg/slice"
	"github.com/sirupsen/logrus"
)

var (
	skippedNames = map[string]bool{
		"links":   true,
		"actions": true,
	}
)

func (s *Schemas) TypeName(name string, obj interface{}) *Schemas {
	s.typeNames[reflect.TypeOf(obj)] = name
	return s
}

func (s *Schemas) getTypeName(t reflect.Type) string {
	if name, ok := s.typeNames[t]; ok {
		return name
	}
	return convert.LowerTitle(t.Name())
}

func (s *Schemas) SchemaFor(t reflect.Type) *Schema {
	name := s.getTypeName(t)
	return s.Schema(name)
}

func (s *Schemas) AddMapperForType(obj interface{}, mapper ...Mapper) *Schemas {
	if len(mapper) == 0 {
		return s
	}

	t := reflect.TypeOf(obj)
	typeName := s.getTypeName(t)
	if len(mapper) == 1 {
		return s.AddMapper(typeName, mapper[0])
	}
	return s.AddMapper(typeName, Mappers(mapper))
}

func (s *Schemas) MustImport(obj interface{}, externalOverrides ...interface{}) *Schemas {
	if reflect.ValueOf(obj).Kind() == reflect.Ptr {
		panic(fmt.Errorf("obj cannot be a pointer"))
	}

	if _, err := s.Import(obj, externalOverrides...); err != nil {
		panic(err)
	}
	return s
}

func (s *Schemas) MustImportAndCustomize(obj interface{}, f func(*Schema), externalOverrides ...interface{}) *Schemas {
	return s.MustImport(obj, externalOverrides...).
		MustCustomizeType(obj, f)
}

func getType(obj interface{}) reflect.Type {
	if t, ok := obj.(reflect.Type); ok {
		return t
	}

	t := reflect.TypeOf(obj)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t
}

func (s *Schemas) Import(obj interface{}, externalOverrides ...interface{}) (*Schema, error) {
	var types []reflect.Type
	for _, override := range externalOverrides {
		types = append(types, getType(override))
	}

	t := getType(obj)
	return s.importType(t, types...)
}

func (s *Schemas) newSchemaFromType(t reflect.Type, typeName string) (*Schema, error) {
	schema := &Schema{
		ID:                typeName,
		CodeName:          t.Name(),
		PkgName:           t.PkgPath(),
		ResourceFields:    map[string]Field{},
		ResourceActions:   map[string]Action{},
		CollectionActions: map[string]Action{},
		Attributes:        map[string]interface{}{},
	}

	s.processingTypes[t] = schema
	defer delete(s.processingTypes, t)

	if err := s.readFields(schema, t); err != nil {
		return nil, err
	}

	return schema, nil
}

func (s *Schemas) MustCustomizeType(obj interface{}, f func(*Schema)) *Schemas {
	name := s.getTypeName(reflect.TypeOf(obj))
	schema := s.Schema(name)
	if schema == nil {
		panic("Failed to find schema " + name)
	}

	f(schema)

	return s
}

func (s *Schemas) assignMappers(schema *Schema) error {
	if schema.Mapper != nil {
		return nil
	}

	mappers := s.mapper(schema.ID)
	if canList(schema) {
		if s.DefaultMapper != nil {
			mappers = append([]Mapper{s.DefaultMapper()}, mappers...)
		}
		if s.DefaultPostMapper != nil {
			mappers = append(mappers, s.DefaultPostMapper())
		}
	}

	if len(mappers) > 0 {
		schema.InternalSchema = schema.DeepCopy()
	}

	mapper := &typeMapper{
		Mappers: mappers,
		root:    canList(schema),
	}

	if err := mapper.ModifySchema(schema, s); err != nil {
		return err
	}

	schema.Mapper = mapper
	return nil
}

func canList(schema *Schema) bool {
	return slice.ContainsString(schema.CollectionMethods, "GET")
}

func (s *Schemas) importType(t reflect.Type, overrides ...reflect.Type) (*Schema, error) {
	typeName := s.getTypeName(t)

	existing := s.Schema(typeName)
	if existing != nil {
		return existing, nil
	}

	if s, ok := s.processingTypes[t]; ok {
		logrus.Debugf("Returning half built schema %s for %v", typeName, t)
		return s, nil
	}

	logrus.Tracef("Inspecting schema %s for %v", typeName, t)

	schema, err := s.newSchemaFromType(t, typeName)
	if err != nil {
		return nil, err
	}

	for _, override := range overrides {
		if err := s.readFields(schema, override); err != nil {
			return nil, err
		}
	}

	if err := s.assignMappers(schema); err != nil {
		return nil, err
	}

	err = s.AddSchema(*schema)
	return s.Schema(schema.ID), err
}

func jsonName(f reflect.StructField) string {
	return strings.SplitN(f.Tag.Get("json"), ",", 2)[0]
}

func k8sType(field reflect.StructField) bool {
	return field.Type.Name() == "TypeMeta" &&
		strings.HasSuffix(field.Type.PkgPath(), "k8s.io/apimachinery/pkg/apis/meta/v1")
}

func k8sObject(field reflect.StructField) bool {
	return field.Type.Name() == "ObjectMeta" &&
		strings.HasSuffix(field.Type.PkgPath(), "k8s.io/apimachinery/pkg/apis/meta/v1")
}

func (s *Schemas) readFields(schema *Schema, t reflect.Type) error {
	hasType := false
	hasMeta := false

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		if field.PkgPath != "" {
			// unexported field
			continue
		}

		jsonName := jsonName(field)
		if jsonName == "-" {
			continue
		}

		if field.Anonymous && jsonName == "" && k8sType(field) {
			hasType = true
		}

		if field.Anonymous && jsonName == "metadata" && k8sObject(field) {
			hasMeta = true
		}

		if field.Anonymous && jsonName == "" {
			t := field.Type
			if t.Kind() == reflect.Ptr {
				t = t.Elem()
			}
			if t.Kind() == reflect.Struct {
				if err := s.readFields(schema, t); err != nil {
					return err
				}
			}
			continue
		}

		fieldName := jsonName
		if fieldName == "" {
			fieldName = convert.LowerTitle(field.Name)
			if strings.HasSuffix(fieldName, "ID") {
				fieldName = strings.TrimSuffix(fieldName, "ID") + "Id"
			}
		}

		if skippedNames[fieldName] {
			logrus.Debugf("Ignoring skip field %s.%s for %v", schema.ID, fieldName, field)
			continue
		}

		logrus.Tracef("Inspecting field %s.%s for %v", schema.ID, fieldName, field)

		schemaField := Field{
			CodeName: field.Name,
			Create:   true,
			Update:   true,
		}

		fieldType := field.Type
		if fieldType.Kind() == reflect.Ptr {
			schemaField.Nullable = true
			fieldType = fieldType.Elem()
		} else if fieldType.Kind() == reflect.Bool {
			schemaField.Nullable = false
		} else if fieldType.Kind() == reflect.Int ||
			fieldType.Kind() == reflect.Uint32 ||
			fieldType.Kind() == reflect.Int32 ||
			fieldType.Kind() == reflect.Uint64 ||
			fieldType.Kind() == reflect.Int64 ||
			fieldType.Kind() == reflect.Float32 ||
			fieldType.Kind() == reflect.Float64 {
			schemaField.Nullable = false
		}

		if err := applyTag(&field, &schemaField); err != nil {
			return err
		}

		if schemaField.Type == "" {
			inferredType, err := s.determineSchemaType(fieldType)
			if err != nil {
				return fmt.Errorf("failed inspecting type %s, field %s: %v", t, fieldName, err)
			}
			schemaField.Type = inferredType
		}

		if schemaField.Default != nil {
			switch schemaField.Type {
			case "int":
				n, err := convert.ToNumber(schemaField.Default)
				if err != nil {
					return err
				}
				schemaField.Default = n
			case "float":
				n, err := convert.ToFloat(schemaField.Default)
				if err != nil {
					return err
				}
				schemaField.Default = n
			case "boolean":
				schemaField.Default = convert.ToBool(schemaField.Default)
			}
		}

		if s.fieldMappers != nil {
			if err := s.processFieldsMappers(t, fieldName, schema, field); err != nil {
				return err
			}
		}

		logrus.Tracef("Setting field %s.%s: %#v", schema.ID, fieldName, schemaField)
		schema.ResourceFields[fieldName] = schemaField
	}

	if hasType && hasMeta {
		delete(schema.ResourceFields, "kind")
		delete(schema.ResourceFields, "apiVersion")
		delete(schema.ResourceFields, "metadata")
		schema.CollectionMethods = []string{"GET", "POST"}
		schema.ResourceMethods = []string{"GET", "PUT", "DELETE"}
	}

	return nil
}

func (s *Schemas) processFieldsMappers(t reflect.Type, fieldName string, schema *Schema, field reflect.StructField) error {
	for _, fieldMapper := range strings.Split(field.Tag.Get("mapper"), ",") {
		if fieldMapper == "" {
			continue
		}

		var (
			name string
			opts []string
		)
		parts := strings.SplitN(fieldMapper, "=", 2)
		name = parts[0]
		if len(parts) == 2 {
			for _, opt := range strings.Split(parts[1], "|") {
				opts = append(opts, opt)
			}
		}

		factory, ok := s.fieldMappers[name]
		if !ok {
			return fmt.Errorf("failed to find field mapper [%s] for type [%v]", name, t)
		}

		s.AddMapper(schema.ID, factory(fieldName, opts...))
	}

	return nil
}

func applyTag(structField *reflect.StructField, field *Field) error {
	t := structField.Tag.Get("wrangler")
	for _, part := range strings.Split(t, ",") {
		if part == "" {
			continue
		}

		var err error
		key, value := getKeyValue(part)

		switch key {
		case "type":
			field.Type = value
		case "codeName":
			field.CodeName = value
		case "default":
			field.Default = value
		case "nullable":
			field.Nullable = true
		case "notnullable":
			field.Nullable = false
		case "create":
			field.Create = true
		case "nocreate":
			field.Create = false
		case "writeOnly":
			field.WriteOnly = true
		case "required":
			field.Required = true
		case "update":
			field.Update = true
		case "noupdate":
			field.Update = false
		case "minLength":
			field.MinLength, err = toInt(value, structField)
		case "maxLength":
			field.MaxLength, err = toInt(value, structField)
		case "min":
			field.Min, err = toInt(value, structField)
		case "max":
			field.Max, err = toInt(value, structField)
		case "options":
			field.Options = split(value)
			if field.Type == "" {
				field.Type = "enum"
			}
		case "validChars":
			field.ValidChars = value
		case "invalidChars":
			field.InvalidChars = value
		default:
			return fmt.Errorf("invalid tag %s on field %s", key, structField.Name)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func toInt(value string, structField *reflect.StructField) (*int64, error) {
	i, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid number on field %s: %v", structField.Name, err)
	}
	return &i, nil
}

func split(input string) []string {
	var result []string
	for _, i := range strings.Split(input, "|") {
		for _, part := range strings.Split(i, " ") {
			part = strings.TrimSpace(part)
			if len(part) > 0 {
				result = append(result, part)
			}
		}
	}

	return result
}

func getKeyValue(input string) (string, string) {
	var (
		key, value string
	)
	parts := strings.SplitN(input, "=", 2)
	key = parts[0]
	if len(parts) > 1 {
		value = parts[1]
	}

	return key, value
}

func deRef(p reflect.Type) reflect.Type {
	if p.Kind() == reflect.Ptr {
		return p.Elem()
	}
	return p
}

func (s *Schemas) determineSchemaType(t reflect.Type) (string, error) {
	switch t.Kind() {
	case reflect.Uint8:
		return "byte", nil
	case reflect.Bool:
		return "boolean", nil
	case reflect.Int:
		fallthrough
	case reflect.Int32:
		fallthrough
	case reflect.Uint32:
		fallthrough
	case reflect.Uint64:
		fallthrough
	case reflect.Int64:
		return "int", nil
	case reflect.Float32:
		fallthrough
	case reflect.Float64:
		return "float", nil
	case reflect.Interface:
		return "json", nil
	case reflect.Map:
		subType, err := s.determineSchemaType(deRef(t.Elem()))
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("map[%s]", subType), nil
	case reflect.Slice:
		subType, err := s.determineSchemaType(deRef(t.Elem()))
		if err != nil {
			return "", err
		}
		if subType == "byte" {
			return "base64", nil
		}
		return fmt.Sprintf("array[%s]", subType), nil
	case reflect.String:
		return "string", nil
	case reflect.Struct:
		if t.Name() == "Time" {
			return "date", nil
		}
		if t.Name() == "IntOrString" {
			return "intOrString", nil
		}
		if t.Name() == "Quantity" {
			return "string", nil
		}
		schema, err := s.importType(t)
		if err != nil {
			return "", err
		}
		if t.Name() == "Duration" && strings.Contains(schema.PkgName, "k8s.io/apimachinery/pkg/apis/meta/v1") {
			return "string", nil
		}
		return schema.ID, nil
	default:
		return "", fmt.Errorf("unknown type kind %s", t.Kind())
	}

}
