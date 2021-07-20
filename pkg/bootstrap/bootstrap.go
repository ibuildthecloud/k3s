package bootstrap

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"github.com/rancher/k3s/pkg/daemons/config"
	"github.com/sirupsen/logrus"
)

func Handler(bootstrap *config.ControlRuntimeBootstrap) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		ReadFromDisk(rw, bootstrap)
	})
}

func ReadFromDisk(w io.Writer, bootstrap *config.ControlRuntimeBootstrap) error {
	paths, err := objToMap(bootstrap)
	if err != nil {
		return nil
	}

	dataMap := make(map[string]bootstrapFile)
	for pathKey, path := range paths {
		if path == "" {
			continue
		}
		data, err := ioutil.ReadFile(path)
		if err != nil {
			return errors.Wrapf(err, "failed to read %s", path)
		}

		info, err := os.Stat(path)
		if err != nil {
			return err
		}

		dataMap[pathKey] = bootstrapFile{
			timestamp: info.ModTime(),
			content:   data,
		}
	}

	return json.NewEncoder(w).Encode(dataMap)
}

func ReconcileStorage(r io.Reader, bootstrap *config.ControlRuntimeBootstrap) error {
	paths, err := objToMap(bootstrap)
	if err != nil {
		return err
	}

	files := make(map[string]bootstrapFile)
	if err := json.NewDecoder(r).Decode(&files); err != nil {
		return err
	}

	for pathKey, data := range files {
		path, ok := paths[pathKey]
		if !ok {
			continue
		}

		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()

		fd, err := ioutil.ReadAll(f)
		if err != nil {
			return err
		}

		if !bytes.Equal(data.content, fd) {
			logrus.Warn("%s and database out of sync", path)

			info, err := os.Stat(path)
			if err != nil {
				return err
			}

			if info.ModTime().After(files[pathKey].timestamp) {
				logrus.Warnf("XXX - on disk file newer than database")
			} else {
				logrus.Warnf("XXX - database newer than on disk file")
			}
		}
	}

	return nil
}

// func WriteBootstrapFiles() error {
// 	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
// 		return errors.Wrapf(err, "failed to mkdir %s", filepath.Dir(path))
// 	}

// 	return nil
// }

// isDirEmpty checks to see if the given directory
// is empty.
func isDirEmpty(name string) (bool, error) {
	f, err := os.Open(name)
	if err != nil {
		return false, err
	}
	defer f.Close()

	_, err = f.Readdir(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err
}

// bootstrapFile
type bootstrapFile struct {
	timestamp time.Time
	content   []byte
}

// paths = map[struct-field-name]path(${data-dir}/server/tls/<filename>)
// files = map[struct-field-name]file-data
// range over files -
// for each file, we get the path from "paths" and write files content
// files map change to map[string]bootstrapFile

// Compare
// 1. Is there a different between db and disk?
//   A. If so, which direction?
//      1. Which is newer?
//      2. Update the older of the 2

func WriteToDisk(r io.Reader, bootstrap *config.ControlRuntimeBootstrap) error {
	paths, err := objToMap(bootstrap)
	if err != nil {
		return err
	}

	files := make(map[string]bootstrapFile)
	if err := json.NewDecoder(r).Decode(&files); err != nil {
		return err
	}

	for pathKey, bsf := range files {
		path, ok := paths[pathKey]
		if !ok {
			continue
		}

		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			return errors.Wrapf(err, "failed to mkdir %s", filepath.Dir(path))
		}
		if err := ioutil.WriteFile(path, bsf.content, 0600); err != nil {
			return errors.Wrapf(err, "failed to write to %s", path)
		}
	}

	return nil
}

func objToMap(obj interface{}) (map[string]string, error) {
	bytes, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	data := map[string]string{}
	return data, json.Unmarshal(bytes, &data)
}
