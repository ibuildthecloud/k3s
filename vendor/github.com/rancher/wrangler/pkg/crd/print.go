package crd

import (
	"context"
	"io"
	"os"
	"path/filepath"

	"github.com/rancher/wrangler/pkg/yaml"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
)

func WriteFile(filename string, crds []CRD) error {
	if err := os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return err
	}
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	return Print(f, crds)
}

func Print(out io.Writer, crds []CRD) error {
	obj, err := Objects(crds)
	if err != nil {
		return err
	}

	data, err := yaml.Export(obj...)
	if err != nil {
		return err
	}

	_, err = out.Write(data)
	return err
}

func Objects(crds []CRD) (result []runtime.Object, err error) {
	for _, crdDef := range crds {
		if crdDef.Override == nil {
			crd, err := crdDef.ToCustomResourceDefinition()
			if err != nil {
				return nil, err
			}
			result = append(result, crd)
		} else {
			result = append(result, crdDef.Override)
		}
	}
	return
}

func Create(ctx context.Context, cfg *rest.Config, crds []CRD) error {
	factory, err := NewFactoryFromClient(cfg)
	if err != nil {
		return err
	}

	return factory.BatchCreateCRDs(ctx, crds...).BatchWait()
}
