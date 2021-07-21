package generators

import (
	"fmt"
	"io"

	args2 "github.com/rancher/wrangler/pkg/controller-gen/args"
	"k8s.io/gengo/args"
	"k8s.io/gengo/generator"
	"k8s.io/gengo/namer"
)

func GroupInterfaceGo(group string, args *args.GeneratorArgs, customArgs *args2.CustomArgs) generator.Generator {
	return &interfaceGo{
		group:      group,
		args:       args,
		customArgs: customArgs,
		DefaultGen: generator.DefaultGen{
			OptionalName: "interface",
			OptionalBody: []byte(interfaceBody),
		},
	}
}

type interfaceGo struct {
	generator.DefaultGen

	group      string
	args       *args.GeneratorArgs
	customArgs *args2.CustomArgs
}

func (f *interfaceGo) Imports(context *generator.Context) []string {
	packages := Imports

	for gv := range f.customArgs.TypesByGroup {
		if gv.Group != f.group {
			continue
		}
		packages = append(packages, fmt.Sprintf("%s \"%s/controllers/%s/%s\"", gv.Version, f.customArgs.Package,
			groupPackageName(gv.Group, f.customArgs.Options.Groups[gv.Group].OutputControllerPackageName), gv.Version))
	}

	return packages
}

func (f *interfaceGo) Init(c *generator.Context, w io.Writer) error {
	sw := generator.NewSnippetWriter(w, c, "{{", "}}")
	sw.Do("type Interface interface {\n", nil)
	for gv := range f.customArgs.TypesByGroup {
		if gv.Group != f.group {
			continue
		}

		sw.Do("{{.upperVersion}}() {{.version}}.Interface\n", map[string]interface{}{
			"upperVersion": namer.IC(gv.Version),
			"version":      gv.Version,
		})
	}
	sw.Do("}\n", nil)

	if err := f.DefaultGen.Init(c, w); err != nil {
		return err
	}

	for gv := range f.customArgs.TypesByGroup {
		if gv.Group != f.group {
			continue
		}

		m := map[string]interface{}{
			"upperGroup":   upperLowercase(f.group),
			"upperVersion": namer.IC(gv.Version),
			"version":      gv.Version,
		}
		sw.Do("\nfunc (g *group) {{.upperVersion}}() {{.version}}.Interface {\n", m)
		sw.Do("return {{.version}}.New(g.controllerFactory)\n", m)
		sw.Do("}\n", m)
	}

	return sw.Error()
}

var interfaceBody = `
type group struct {
	controllerFactory controller.SharedControllerFactory
}

// New returns a new Interface.
func New(controllerFactory controller.SharedControllerFactory) Interface {
	return &group{
		controllerFactory: controllerFactory,
	}
}
`
