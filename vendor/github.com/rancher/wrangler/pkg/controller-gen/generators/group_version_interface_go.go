package generators

import (
	"fmt"
	"io"
	"strings"

	args2 "github.com/rancher/wrangler/pkg/controller-gen/args"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/gengo/args"
	"k8s.io/gengo/generator"
	"k8s.io/gengo/namer"
	"k8s.io/gengo/types"
)

func GroupVersionInterfaceGo(gv schema.GroupVersion, args *args.GeneratorArgs, customArgs *args2.CustomArgs) generator.Generator {
	return &groupInterfaceGo{
		gv:         gv,
		args:       args,
		customArgs: customArgs,
		DefaultGen: generator.DefaultGen{
			OptionalName: "interface",
		},
	}
}

type groupInterfaceGo struct {
	generator.DefaultGen

	gv         schema.GroupVersion
	args       *args.GeneratorArgs
	customArgs *args2.CustomArgs
}

func (f *groupInterfaceGo) Imports(context *generator.Context) []string {
	firstType := f.customArgs.TypesByGroup[f.gv][0]

	packages := append(Imports,
		fmt.Sprintf("%s \"%s\"", f.gv.Version, firstType.Package))

	return packages
}

var (
	pluralExceptions = map[string]string{
		"Endpoints": "Endpoints",
	}
	plural = namer.NewPublicPluralNamer(pluralExceptions)
)

func (f *groupInterfaceGo) Init(c *generator.Context, w io.Writer) error {
	sw := generator.NewSnippetWriter(w, c, "{{", "}}")

	orderer := namer.Orderer{Namer: namer.NewPrivateNamer(0)}

	var types []*types.Type
	for _, name := range f.customArgs.TypesByGroup[f.gv] {
		types = append(types, c.Universe.Type(*name))
	}
	types = orderer.OrderTypes(types)

	sw.Do("func init() {\n", nil)
	sw.Do("schemes.Register("+f.gv.Version+".AddToScheme)\n", nil)
	sw.Do("}\n", nil)

	sw.Do("type Interface interface {\n", nil)
	for _, t := range types {
		m := map[string]interface{}{
			"type": t.Name.Name,
		}
		sw.Do("{{.type}}() {{.type}}Controller\n", m)
	}
	sw.Do("}\n", nil)

	m := map[string]interface{}{
		"version":      f.gv.Version,
		"versionUpper": namer.IC(f.gv.Version),
		"groupUpper":   upperLowercase(f.gv.Group),
	}
	sw.Do(groupInterfaceBody, m)

	for _, t := range types {
		m := map[string]interface{}{
			"type":         t.Name.Name,
			"plural":       plural.Name(t),
			"pluralLower":  strings.ToLower(plural.Name(t)),
			"version":      f.gv.Version,
			"group":        f.gv.Group,
			"namespaced":   namespaced(t),
			"versionUpper": namer.IC(f.gv.Version),
			"groupUpper":   upperLowercase(f.gv.Group),
		}

		sw.Do("func (c *version) {{.type}}() {{.type}}Controller {\n", m)
		sw.Do("return New{{.type}}Controller(schema.GroupVersionKind{Group:\"{{.group}}\",Version:\"{{.version}}\",Kind:\"{{.type}}\"}	, \"{{.pluralLower}}\", {{.namespaced}}, c.controllerFactory)\n", m)
		sw.Do("}\n", m)
	}

	return sw.Error()
}

var groupInterfaceBody = `
func New(controllerFactory controller.SharedControllerFactory) Interface {
	return &version{
		controllerFactory: controllerFactory,
	}
}

type version struct {
	controllerFactory controller.SharedControllerFactory
}

`
