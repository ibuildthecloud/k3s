/*
Copyright 2015 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package generators

import (
	"fmt"
	"path/filepath"
	"strings"

	args2 "github.com/rancher/wrangler/pkg/controller-gen/args"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/gengo/args"
	"k8s.io/gengo/generator"
	"k8s.io/gengo/types"
)

type ClientGenerator struct {
	Fakes map[string][]string
}

func NewClientGenerator() *ClientGenerator {
	return &ClientGenerator{
		Fakes: make(map[string][]string),
	}
}

// Packages makes the client package definition.
func (cg *ClientGenerator) Packages(context *generator.Context, arguments *args.GeneratorArgs) generator.Packages {
	customArgs := arguments.CustomArgs.(*args2.CustomArgs)
	generateTypesGroups := map[string]bool{}

	for groupName, group := range customArgs.Options.Groups {
		if group.GenerateTypes {
			generateTypesGroups[groupName] = true
		}
	}

	var (
		packageList []generator.Package
		groups      = map[string]bool{}
	)

	for gv, types := range customArgs.TypesByGroup {
		if !groups[gv.Group] {
			packageList = append(packageList, cg.groupPackage(gv.Group, arguments, customArgs))
			if generateTypesGroups[gv.Group] {
				packageList = append(packageList, cg.typesGroupPackage(types[0], gv, arguments, customArgs))
			}
		}
		groups[gv.Group] = true
		packageList = append(packageList, cg.groupVersionPackage(gv, arguments, customArgs))

		if generateTypesGroups[gv.Group] {
			packageList = append(packageList, cg.typesGroupVersionPackage(types[0], gv, arguments, customArgs))
			packageList = append(packageList, cg.typesGroupVersionDocPackage(types[0], gv, arguments, customArgs))
		}
	}

	return packageList
}

func (cg *ClientGenerator) typesGroupPackage(name *types.Name, gv schema.GroupVersion, generatorArgs *args.GeneratorArgs, customArgs *args2.CustomArgs) generator.Package {
	packagePath := strings.TrimRight(name.Package, "/"+gv.Version)
	return Package(generatorArgs, packagePath, func(context *generator.Context) []generator.Generator {
		return []generator.Generator{
			RegisterGroupGo(gv.Group, generatorArgs, customArgs),
		}
	})
}

func (cg *ClientGenerator) typesGroupVersionDocPackage(name *types.Name, gv schema.GroupVersion, generatorArgs *args.GeneratorArgs, customArgs *args2.CustomArgs) generator.Package {
	packagePath := name.Package
	p := Package(generatorArgs, packagePath, func(context *generator.Context) []generator.Generator {
		return []generator.Generator{
			generator.DefaultGen{
				OptionalName: "doc",
			},
			RegisterGroupVersionGo(gv, generatorArgs, customArgs),
			ListTypesGo(gv, generatorArgs, customArgs),
		}
	})

	p.(*generator.DefaultPackage).HeaderText = append(p.(*generator.DefaultPackage).HeaderText, []byte(fmt.Sprintf(`

// +k8s:deepcopy-gen=package
// +groupName=%s
`, gv.Group))...)

	return p
}

func (cg *ClientGenerator) typesGroupVersionPackage(name *types.Name, gv schema.GroupVersion, generatorArgs *args.GeneratorArgs, customArgs *args2.CustomArgs) generator.Package {
	packagePath := name.Package
	return Package(generatorArgs, packagePath, func(context *generator.Context) []generator.Generator {
		return []generator.Generator{
			RegisterGroupVersionGo(gv, generatorArgs, customArgs),
			ListTypesGo(gv, generatorArgs, customArgs),
		}
	})
}

func (cg *ClientGenerator) groupPackage(group string, generatorArgs *args.GeneratorArgs, customArgs *args2.CustomArgs) generator.Package {
	packagePath := filepath.Join(customArgs.Package, "controllers", groupPackageName(group, customArgs.Options.Groups[group].OutputControllerPackageName))
	return Package(generatorArgs, packagePath, func(context *generator.Context) []generator.Generator {
		return []generator.Generator{
			FactoryGo(group, generatorArgs, customArgs),
			GroupInterfaceGo(group, generatorArgs, customArgs),
		}
	})
}

func (cg *ClientGenerator) groupVersionPackage(gv schema.GroupVersion, generatorArgs *args.GeneratorArgs, customArgs *args2.CustomArgs) generator.Package {
	packagePath := filepath.Join(customArgs.Package, "controllers", groupPackageName(gv.Group, customArgs.Options.Groups[gv.Group].OutputControllerPackageName), gv.Version)

	return Package(generatorArgs, packagePath, func(context *generator.Context) []generator.Generator {
		generators := []generator.Generator{
			GroupVersionInterfaceGo(gv, generatorArgs, customArgs),
		}

		for _, t := range customArgs.TypesByGroup[gv] {
			generators = append(generators, TypeGo(gv, t, generatorArgs, customArgs))
			cg.Fakes[packagePath] = append(cg.Fakes[packagePath], t.Name)
		}

		return generators
	})
}
