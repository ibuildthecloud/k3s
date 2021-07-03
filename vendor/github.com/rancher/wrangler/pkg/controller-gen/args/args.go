package args

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/gengo/types"
)

type CustomArgs struct {
	Package      string
	TypesByGroup map[schema.GroupVersion][]*types.Name
	Options      Options
	OutputBase   string
}

type Options struct {
	OutputPackage string
	Groups        map[string]Group
	Boilerplate   string
}

type Type struct {
	Version string
	Package string
	Name    string
}

type Group struct {
	// Types is a slice of the following types
	// Instance of any struct: used for reflection to describe the type
	// string: a directory that will be listed (non-recursively) for types
	// Type: a description of a type
	Types         []interface{}
	GenerateTypes bool
	// Generate clientsets
	GenerateClients             bool
	OutputControllerPackageName string
	// Generate listers
	GenerateListers bool
	// Generate informers
	GenerateInformers bool
	// The package name of the API types
	PackageName string
	// Use existing clientset, informer, listers
	ClientSetPackage string
	ListersPackage   string
	InformersPackage string
}
