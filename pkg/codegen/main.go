package main

import (
	"os"

	bindata "github.com/go-bindata/go-bindata"
	v1 "github.com/rancher/k3s/pkg/apis/k3s.cattle.io/v1"
	controllergen "github.com/rancher/wrangler/pkg/controller-gen"
	"github.com/rancher/wrangler/pkg/controller-gen/args"
	"github.com/sirupsen/logrus"
)

var (
	basePackage = "github.com/rancher/k3s/types"
)

func main() {
	os.Unsetenv("GOPATH")
	bc := &bindata.Config{
		Input: []bindata.InputConfig{
			{
				Path:      "build/data",
				Recursive: true,
			},
		},
		Package:    "data",
		NoCompress: true,
		NoMemCopy:  true,
		NoMetadata: true,
		Output:     "pkg/data/zz_generated_bindata.go",
	}
	if err := bindata.Translate(bc); err != nil {
		logrus.Fatal(err)
	}

	bc = &bindata.Config{
		Input: []bindata.InputConfig{
			{
				Path:      "manifests",
				Recursive: true,
			},
		},
		Package:    "deploy",
		NoMetadata: true,
		Prefix:     "manifests/",
		Output:     "pkg/deploy/zz_generated_bindata.go",
		Tags:       "!no_stage",
	}
	if err := bindata.Translate(bc); err != nil {
		logrus.Fatal(err)
	}

	bc = &bindata.Config{
		Input: []bindata.InputConfig{
			{
				Path:      "build/static",
				Recursive: true,
			},
		},
		Package:    "static",
		NoMetadata: true,
		Prefix:     "build/static/",
		Output:     "pkg/static/zz_generated_bindata.go",
		Tags:       "!no_stage",
	}
	if err := bindata.Translate(bc); err != nil {
		logrus.Fatal(err)
	}

	controllergen.Run(args.Options{
		OutputPackage: "github.com/rancher/k3s/pkg/generated",
		Boilerplate:   "scripts/boilerplate.go.txt",
		Groups: map[string]args.Group{
			"k3s.cattle.io": {
				Types: []interface{}{
					v1.Addon{},
				},
				GenerateTypes:   true,
				GenerateClients: true,
			},
		},
	})
}
