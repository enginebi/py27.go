package py27

// go:generate export PYTHONPATH=$GOPATH/src/github.com/enginebi/py3.go/pyscripts
import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/sbinet/go-python"
	log "github.com/sirupsen/logrus"
)

var (
	module *python.PyObject
	mutex  sync.Mutex
)

type resp [][]float64

func init() {
	log.Printf("evn:%s\n", os.Getenv("PYTHONPATH"))
	err := python.Initialize()
	log.Infof("python.Initialize()...")
	if err != nil {
		log.Errorf("python.Initialize(), err:%+v", err)
		panic(err.Error())
	}
}

func ToPyTuple(vs ...float64) *python.PyObject {
	args := python.PyTuple_New(len(vs))
	for i, v := range vs {
		python.PyTuple_SetItem(args, i, python.PyFloat_FromDouble(v))
	}
	return args
}

func ToPyListV2(input [][]float64) *python.PyObject {
	args := python.PyTuple_New(len(input))
	for i, it := range input {
		subargs := python.PyList_New(0)
		for _, jt := range it {
			python.PyList_Append(subargs, python.PyFloat_FromDouble(jt))
		}
		python.PyTuple_SetItem(args, i, subargs)
	}
	return args
}

func ToPyList(input [][]float64) *python.PyObject {
	args := python.PyList_New(0)
	for _, it := range input {
		subargs := python.PyList_New(0)
		for _, jt := range it {
			python.PyList_Append(subargs, python.PyFloat_FromDouble(jt))
		}
		python.PyList_Append(args, subargs)
	}
	return args
}

func ToPyDictV2(vs map[string]int32) *python.PyObject {
	args := python.PyDict_New()
	for k, v := range vs {
		python.PyDict_SetItem(
			args,
			python.PyString_FromString(k),
			python.PyInt_FromLong(int(v)),
		)
	}
	return args
}

func ToPyDict(vs ...float64) *python.PyObject {
	args := python.PyDict_New()
	for i, v := range vs {
		python.PyDict_SetItem(
			args,
			python.PyString_FromString(strconv.FormatInt(int64(i), 10)),
			python.PyFloat_FromDouble(v),
		)
	}
	return args
}

func ToGoSlice(out string) []string {
	s1 := strings.Split(out, "),")
	if len(s1) <= 0 {
		return nil
	}
	return strings.Split(strings.Trim(s1[0], "(("), ", ")
}

func AtoFs(strs []string) []float64 {
	ret := make([]float64, 0, len(strs))
	for _, str := range strs {
		f, err := strconv.ParseFloat(str, 10)
		if err != nil {
			log.Printf("parse %s, err:%+v", str, err)
			continue
		}
		ret = append(ret, f)
	}
	return ret
}

func Init(m string) *python.PyObject {
	log.Infof("ImportModule:%s", m)
	module = python.PyImport_ImportModule(m)
	if module == nil {
		log.Fatalf("could not import '%s'", m)
	}
	return module
}

func GoPyFuncV2(funcname string, args [][]float64, params map[string]int32) ([][]float64, error) {
	var out *python.PyObject
	{
		mutex.Lock() // 加锁原因：并发不安全 https://cloud.tencent.com/developer/article/1149341
		defer mutex.Unlock()
		fname := module.GetAttrString(funcname)
		if fname == nil {
			err := fmt.Errorf("could not getattr(%s, '%s')", funcname, funcname)
			log.Error(err)
			return nil, err
		}
		log.Debugf("GoPyFuncV2, %s", funcname)

		pyargs := ToPyListV2(args)
		pyparams := ToPyDictV2(params)
		log.Infof("fname:%+v", *fname)
		log.Infof("pyargs:%+v", pyargs)
		log.Infof("pyparams:%+v", pyparams)

		out = fname.Call(pyargs, pyparams)
		log.Infof("out:%+v", out)
	}

	var r resp
	err := json.Unmarshal([]byte(out.Bytes().String()), &r)
	if err != nil {
		log.Errorf("err:%+v", err)
		return nil, err
	}
	log.Infof("resp:%+v", r)
	return [][]float64(r), nil
}

func GoPyFunc(funcname string, args ...float64) []float64 {
	fname := module.GetAttrString(funcname)
	if fname == nil {
		log.Fatalf("could not getattr(%s, '%s')\n", funcname, funcname)
	}
	log.Debugf("GoPyFunc, %s", funcname)

	pyargs := ToPyTuple(args...)
	pyparams := ToPyDict(args...)
	log.Infof("fname:%+v", *fname)
	log.Infof("pyargs:%+v", pyargs)
	log.Infof("pyparams:%+v", pyparams)

	out := fname.Call(pyargs, pyparams)
	log.Infof("out:%+v", out)

	strs := ToGoSlice(out.Bytes().String())
	return AtoFs(strs)
}
