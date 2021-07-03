package controllergen

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"k8s.io/gengo/generator"

	cgargs "github.com/rancher/wrangler/pkg/controller-gen/args"
	"github.com/rancher/wrangler/pkg/controller-gen/generators"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/runtime/schema"
	csargs "k8s.io/code-generator/cmd/client-gen/args"
	clientgenerators "k8s.io/code-generator/cmd/client-gen/generators"
	cs "k8s.io/code-generator/cmd/client-gen/generators"
	types2 "k8s.io/code-generator/cmd/client-gen/types"
	dpargs "k8s.io/code-generator/cmd/deepcopy-gen/args"
	infargs "k8s.io/code-generator/cmd/informer-gen/args"
	inf "k8s.io/code-generator/cmd/informer-gen/generators"
	lsargs "k8s.io/code-generator/cmd/lister-gen/args"
	ls "k8s.io/code-generator/cmd/lister-gen/generators"
	"k8s.io/gengo/args"
	dp "k8s.io/gengo/examples/deepcopy-gen/generators"
	"k8s.io/gengo/types"
)

var (
	t = true
)

func Run(opts cgargs.Options) {
	customArgs := &cgargs.CustomArgs{
		Options:      opts,
		TypesByGroup: map[schema.GroupVersion][]*types.Name{},
		Package:      opts.OutputPackage,
	}

	genericArgs := args.Default().WithoutDefaultFlagParsing()
	genericArgs.CustomArgs = customArgs
	genericArgs.GoHeaderFilePath = opts.Boilerplate
	genericArgs.InputDirs = parseTypes(customArgs)

	if genericArgs.OutputBase == "./" { //go modules
		tempDir, err := ioutil.TempDir("", "")
		if err != nil {
			return
		}

		genericArgs.OutputBase = tempDir
		defer os.RemoveAll(tempDir)
	}
	customArgs.OutputBase = genericArgs.OutputBase

	clientGen := generators.NewClientGenerator()

	if err := genericArgs.Execute(
		clientgenerators.NameSystems(nil),
		clientgenerators.DefaultNameSystem(),
		clientGen.Packages,
	); err != nil {
		logrus.Fatalf("Error: %v", err)
	}

	groups := map[string]bool{}
	listerGroups := map[string]bool{}
	informerGroups := map[string]bool{}
	deepCopygroups := map[string]bool{}
	for groupName, group := range customArgs.Options.Groups {
		if group.GenerateTypes {
			deepCopygroups[groupName] = true
		}
		if group.GenerateClients {
			groups[groupName] = true
		}
		if group.GenerateListers {
			listerGroups[groupName] = true
		}
		if group.GenerateInformers {
			informerGroups[groupName] = true
		}
	}

	if len(deepCopygroups) == 0 && len(groups) == 0 && len(listerGroups) == 0 && len(informerGroups) == 0 {
		if err := copyGoPathToModules(customArgs); err != nil {
			logrus.Fatalf("go modules copy failed: %v", err)
		}
		return
	}

	if err := copyGoPathToModules(customArgs); err != nil {
		logrus.Fatalf("go modules copy failed: %v", err)
	}

	if err := generateDeepcopy(deepCopygroups, customArgs); err != nil {
		logrus.Fatalf("deepcopy failed: %v", err)
	}

	if err := generateClientset(groups, customArgs); err != nil {
		logrus.Fatalf("clientset failed: %v", err)
	}

	if err := generateListers(listerGroups, customArgs); err != nil {
		logrus.Fatalf("listers failed: %v", err)
	}

	if err := generateInformers(informerGroups, customArgs); err != nil {
		logrus.Fatalf("informers failed: %v", err)
	}

	if err := copyGoPathToModules(customArgs); err != nil {
		logrus.Fatalf("go modules copy failed: %v", err)
	}
}

func sourcePackagePath(customArgs *cgargs.CustomArgs, pkgName string) string {
	pkgSplit := strings.Split(pkgName, string(os.PathSeparator))
	pkg := filepath.Join(customArgs.OutputBase, strings.Join(pkgSplit[:3], string(os.PathSeparator)))
	return pkg
}

//until k8s code-gen supports gopath
func copyGoPathToModules(customArgs *cgargs.CustomArgs) error {

	pathsToCopy := map[string]bool{}
	for _, types := range customArgs.TypesByGroup {
		for _, names := range types {
			pkg := sourcePackagePath(customArgs, names.Package)
			pathsToCopy[pkg] = true
		}
	}

	pkg := sourcePackagePath(customArgs, customArgs.Package)
	pathsToCopy[pkg] = true

	for pkg, _ := range pathsToCopy {
		if _, err := os.Stat(pkg); os.IsNotExist(err) {
			continue
		}

		return filepath.Walk(pkg, func(path string, info os.FileInfo, err error) error {
			newPath := strings.Replace(path, pkg, ".", 1)
			if info.IsDir() {
				return os.MkdirAll(newPath, info.Mode())
			}

			return copyFile(path, newPath)
		})
	}

	return nil
}

func copyFile(src, dst string) error {
	var err error
	var srcfd *os.File
	var dstfd *os.File
	var srcinfo os.FileInfo

	if srcfd, err = os.Open(src); err != nil {
		return err
	}
	defer srcfd.Close()

	if dstfd, err = os.Create(dst); err != nil {
		return err
	}
	defer dstfd.Close()

	if _, err = io.Copy(dstfd, srcfd); err != nil {
		return err
	}
	if srcinfo, err = os.Stat(src); err != nil {
		return err
	}
	return os.Chmod(dst, srcinfo.Mode())
}

func generateDeepcopy(groups map[string]bool, customArgs *cgargs.CustomArgs) error {
	if len(groups) == 0 {
		return nil
	}

	deepCopyCustomArgs := &dpargs.CustomArgs{}

	args := args.Default().WithoutDefaultFlagParsing()
	args.CustomArgs = deepCopyCustomArgs
	args.OutputBase = customArgs.OutputBase
	args.OutputFileBaseName = "zz_generated_deepcopy"
	args.GoHeaderFilePath = customArgs.Options.Boilerplate

	for gv, names := range customArgs.TypesByGroup {
		if !groups[gv.Group] {
			continue
		}
		args.InputDirs = append(args.InputDirs, names[0].Package)
		deepCopyCustomArgs.BoundingDirs = append(deepCopyCustomArgs.BoundingDirs, names[0].Package)
	}

	return args.Execute(dp.NameSystems(),
		dp.DefaultNameSystem(),
		dp.Packages)
}

func generateClientset(groups map[string]bool, customArgs *cgargs.CustomArgs) error {
	if len(groups) == 0 {
		return nil
	}

	args, clientSetArgs := csargs.NewDefaults()
	clientSetArgs.ClientsetName = "versioned"
	args.OutputBase = customArgs.OutputBase
	args.OutputPackagePath = filepath.Join(customArgs.Package, "clientset")
	args.GoHeaderFilePath = customArgs.Options.Boilerplate

	var order []schema.GroupVersion

	for gv := range customArgs.TypesByGroup {
		if !groups[gv.Group] {
			continue
		}
		order = append(order, gv)
	}
	sort.Slice(order, func(i, j int) bool {
		return order[i].Group < order[j].Group
	})

	for _, gv := range order {
		packageName := customArgs.Options.Groups[gv.Group].PackageName
		if packageName == "" {
			packageName = gv.Group
		}
		names := customArgs.TypesByGroup[gv]
		args.InputDirs = append(args.InputDirs, names[0].Package)
		clientSetArgs.Groups = append(clientSetArgs.Groups, types2.GroupVersions{
			PackageName: packageName,
			Group:       types2.Group(gv.Group),
			Versions: []types2.PackageVersion{
				{
					Version: types2.Version(gv.Version),
					Package: names[0].Package,
				},
			},
		})
	}

	return args.Execute(cs.NameSystems(nil),
		cs.DefaultNameSystem(),
		setGenClient(groups, customArgs.TypesByGroup, cs.Packages))
}

func setGenClient(groups map[string]bool, typesByGroup map[schema.GroupVersion][]*types.Name, f func(*generator.Context, *args.GeneratorArgs) generator.Packages) func(*generator.Context, *args.GeneratorArgs) generator.Packages {
	return func(context *generator.Context, generatorArgs *args.GeneratorArgs) generator.Packages {
		for gv, names := range typesByGroup {
			if !groups[gv.Group] {
				continue
			}
			for _, name := range names {
				var (
					p           = context.Universe.Package(name.Package)
					t           = p.Type(name.Name)
					status      bool
					nsed        bool
					kubebuilder bool
				)

				for _, line := range append(t.SecondClosestCommentLines, t.CommentLines...) {
					switch {
					case strings.Contains(line, "+kubebuilder:object:root=true"):
						kubebuilder = true
						t.SecondClosestCommentLines = append(t.SecondClosestCommentLines, "+genclient")
					case strings.Contains(line, "+kubebuilder:subresource:status"):
						status = true
					case strings.Contains(line, "+kubebuilder:resource:") && strings.Contains(line, "scope=Namespaced"):
						nsed = true
					}
				}

				if kubebuilder {
					if !nsed {
						t.SecondClosestCommentLines = append(t.SecondClosestCommentLines, "+genclient:nonNamespaced")
					}
					if !status {
						t.SecondClosestCommentLines = append(t.SecondClosestCommentLines, "+genclient:noStatus")
					}

					foundGroup := false
					for _, comment := range p.DocComments {
						if strings.Contains(comment, "+groupName=") {
							foundGroup = true
							break
						}
					}

					if !foundGroup {
						p.DocComments = append(p.DocComments, "+groupName="+gv.Group)
						p.Comments = append(p.Comments, "+groupName="+gv.Group)
						fmt.Println(gv.Group, p.DocComments, p.Comments, p.Path)
					}
				}
			}
		}
		return f(context, generatorArgs)
	}
}

func generateInformers(groups map[string]bool, customArgs *cgargs.CustomArgs) error {
	if len(groups) == 0 {
		return nil
	}

	args, clientSetArgs := infargs.NewDefaults()
	clientSetArgs.VersionedClientSetPackage = filepath.Join(customArgs.Package, "clientset/versioned")
	clientSetArgs.ListersPackage = filepath.Join(customArgs.Package, "listers")
	args.OutputBase = customArgs.OutputBase
	args.OutputPackagePath = filepath.Join(customArgs.Package, "informers")
	args.GoHeaderFilePath = customArgs.Options.Boilerplate

	for gv, names := range customArgs.TypesByGroup {
		if !groups[gv.Group] {
			continue
		}
		args.InputDirs = append(args.InputDirs, names[0].Package)
	}

	return args.Execute(inf.NameSystems(nil),
		inf.DefaultNameSystem(),
		setGenClient(groups, customArgs.TypesByGroup, inf.Packages))
}

func generateListers(groups map[string]bool, customArgs *cgargs.CustomArgs) error {
	if len(groups) == 0 {
		return nil
	}

	args, _ := lsargs.NewDefaults()
	args.OutputBase = customArgs.OutputBase
	args.OutputPackagePath = filepath.Join(customArgs.Package, "listers")
	args.GoHeaderFilePath = customArgs.Options.Boilerplate

	for gv, names := range customArgs.TypesByGroup {
		if !groups[gv.Group] {
			continue
		}
		args.InputDirs = append(args.InputDirs, names[0].Package)
	}

	return args.Execute(ls.NameSystems(nil),
		ls.DefaultNameSystem(),
		setGenClient(groups, customArgs.TypesByGroup, ls.Packages))
}

func parseTypes(customArgs *cgargs.CustomArgs) []string {
	for groupName, group := range customArgs.Options.Groups {
		if group.GenerateTypes || group.GenerateClients {
			if group.InformersPackage == "" {
				group.InformersPackage = filepath.Join(customArgs.Package, "informers/externalversions")
			}
			if group.ClientSetPackage == "" {
				group.ClientSetPackage = filepath.Join(customArgs.Package, "clientset/versioned")
			}
			if group.ListersPackage == "" {
				group.ListersPackage = filepath.Join(customArgs.Package, "listers")
			}
			customArgs.Options.Groups[groupName] = group
		}
	}

	for groupName, group := range customArgs.Options.Groups {
		if err := cgargs.ObjectsToGroupVersion(groupName, group.Types, customArgs.TypesByGroup); err != nil {
			// sorry, should really handle this better
			panic(err)
		}
	}

	var inputDirs []string
	for _, names := range customArgs.TypesByGroup {
		inputDirs = append(inputDirs, names[0].Package)
	}

	return inputDirs
}
