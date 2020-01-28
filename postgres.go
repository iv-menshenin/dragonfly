package dragonfly

import (
	"errors"
	"reflect"
	"strconv"
	"strings"
)

type (
	Node interface {
		setFieldValue(s string)
	}
	// args
	BaseNode struct {
		Args []Node // args
	}
	// (
	ArrayNode struct {
		Nodes []Node
	}
	// FUNCEXPR
	FuncNode struct {
		BaseNode
		FuncId         int  `field:"funcid"`
		FuncResultType int  `field:"funcresulttype"`
		FuncRetSet     bool `field:"funcretset"`
	}
	// RELABELTYPE
	ReLabelNode struct {
		Arg           Node
		ResultType    int `field:"resulttype"`
		ResultTypeMod int `field:"resulttypmod"`
		ReLabelFormat int `field:"relabelformat"`
	}
	// VAR
	VarNode struct {
		VarNo       int `field:"varno"`
		VarAttrNo   int `field:"varattno"`
		VarType     int `field:"vartype"`
		VarTypeMod  int `field:"vartypmod"`
		VarLevelSup int `field:"varlevelsup"`
		VarNoOld    int `field:"varnoold"`
		VarOAttNo   int `field:"varoattno"`
	}
	// OPEXPR
	OpNode struct {
		BaseNode
		OpNo         int `field:"opno"`
		OpFuncId     int `field:"opfuncid"`
		OpResultType int `field:"opresulttype"`
		OpRetSet     int `field:"opretset"`
	}
	// CONST
	ConstNode struct {
		ConstType    int    `field:"consttype"`
		ConstTypeMod int    `field:"consttypmod"`
		ConstLen     int    `field:"constlen"`
		ConstByVal   bool   `field:"constbyval"`
		ConstIsNull  bool   `field:"constisnull"`
		ConstValue   []byte `field:"constvalue"`
	}

	chainType int
)

// dummy
func (c *ArrayNode) setFieldValue(s string) {
	panic("not allowed")
}

func (c *FuncNode) setFieldValue(s string) {
	if !setFieldValue(c, s) {
		if del := strings.Index(s, " "); del > 0 {
			fieldName := strings.TrimSpace(s[:del])
			fieldValue := strings.TrimSpace(s[del:])
			if fieldName == "args" {
				c.Args = parsePgNodeTrees(fieldValue)
			}
		}
	}
}

func (c *ReLabelNode) setFieldValue(s string) {
	if !setFieldValue(c, s) {
		if del := strings.Index(s, " "); del > 0 {
			fieldName := strings.TrimSpace(s[:del])
			fieldValue := strings.TrimSpace(s[del:])
			if fieldName == "arg" {
				args := parsePgNodeTrees(fieldValue)
				if len(args) == 1 {
					c.Arg = args[0]
				} else {
					panic("wrong args count")
				}
			}
		}
	}
}

func (c *VarNode) setFieldValue(s string) {
	setFieldValue(c, s)
}

func (c *OpNode) setFieldValue(s string) {
	if !setFieldValue(c, s) {
		if del := strings.Index(s, " "); del > 0 {
			fieldName := strings.TrimSpace(s[:del])
			fieldValue := strings.TrimSpace(s[del:])
			if fieldName == "args" {
				c.Args = parsePgNodeTrees(fieldValue)
			}
		}
	}
}

func (c *ConstNode) setFieldValue(s string) {
	setFieldValue(c, s)
}

func parseBytes(s string) ([]byte, error) {
	s = strings.TrimSpace(s)
	if s[0] != '[' || s[len(s)-1] != ']' {
		return nil, errors.New("data must be enclosed in square brackets")
	}
	f := strings.Fields(s[1 : len(s)-1])
	result := make([]byte, 0, len(f))
	for _, n := range f {
		if i, err := strconv.ParseUint(n, 10, 8); err != nil {
			return nil, err
		} else {
			result = append(result, byte(i))
		}
	}
	return result, nil
}

func setFieldValue(c interface{}, s string) bool {
	var (
		fieldName  = strings.TrimSpace(s)
		fieldValue = ""
	)
	if del := strings.Index(s, " "); del > 0 {
		fieldName = strings.TrimSpace(s[:del])
		fieldValue = strings.TrimSpace(s[del:])
	}
	v := reflect.ValueOf(c)
	t := reflect.TypeOf(c).Elem()
	for nn := 0; nn < t.NumField(); nn++ {
		fieldNameTag := strings.Split(t.Field(nn).Tag.Get("field"), ",")
		if strings.TrimSpace(fieldNameTag[0]) == fieldName {
			if fieldValue == "" {
				v.Elem().Field(nn).Set(reflect.Zero(t.Field(nn).Type))
			} else {
				switch t.Field(nn).Type.Kind() {
				case reflect.Bool:
					b, err := strconv.ParseBool(fieldValue)
					if err != nil {
						panic(err)
					}
					v.Elem().Field(nn).SetBool(b)
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					x, err := strconv.ParseInt(fieldValue, 10, 64)
					if err != nil {
						panic(err)
					}
					v.Elem().Field(nn).SetInt(x)
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					x, err := strconv.ParseUint(fieldValue, 10, 64)
					if err != nil {
						panic(err)
					}
					v.Elem().Field(nn).SetUint(x)
				case reflect.Float32, reflect.Float64:
					x, err := strconv.ParseFloat(fieldValue, 64)
					if err != nil {
						panic(err)
					}
					v.Elem().Field(nn).SetFloat(x)
				case reflect.Slice:
					d, err := parseBytes(fieldValue)
					if err != nil {
						panic(err)
					}
					v.Elem().Field(nn).SetBytes(d)
				case reflect.String:
					v.Elem().Field(nn).SetString(fieldValue)
				default:
					panic("not implemented")
				}
			}
			return true
		}
	}
	return false
}

const (
	FUNCEXPR    = "FUNCEXPR"
	RELABELTYPE = "RELABELTYPE"
	VAR         = "VAR"
	OPEXPR      = "OPEXPR"
	CONST       = "CONST"

	ctNone chainType = iota
	ctArray
	ctObject
	ctData // TODO ?
)

func parsePgNodeTrees(s string) []Node {
	var result = make([]Node, 0, 10)
	for s != "" {
		var (
			chain string
			t     chainType
		)
		chain, s, t = getNextChain(s)
		switch t {
		case ctArray:
			result = append(result, &ArrayNode{Nodes: parsePgNodeTrees(chain)})
		case ctObject:
			result = append(result, parseObject(chain))
		}
	}
	return result
}

func parseObject(s string) Node {
	var node Node
	for s != "" {
		var chain string
		chain, s = getNextField(s)
		if chain != "" {
			if node == nil {
				switch chain {
				case FUNCEXPR:
					node = new(FuncNode)
				case RELABELTYPE:
					node = new(ReLabelNode)
				case VAR:
					node = new(VarNode)
				case OPEXPR:
					node = new(OpNode)
				case CONST:
					node = new(ConstNode)
				default:
					panic("not implemented")
				}
			} else {
				node.setFieldValue(chain)
			}
		}
	}
	return node
}

func getNextField(s string) (chain, left string) {
	s = strings.TrimSpace(s)
	if len(s) < 2 {
		return s, ""
	}
	var (
		nestedTo     uint8 = '\\'
		nestingLevel int
		nn           = 1
	)
	for nn < len(s) {
		switch s[nn] {
		case '(':
			if nestedTo == '\\' {
				nestedTo = ')'
				nestingLevel = 1
			} else if nestedTo == ')' {
				nestingLevel++
			}
		case '{':
			if nestedTo == '\\' {
				nestedTo = '}'
				nestingLevel = 1
			} else if nestedTo == '}' {
				nestingLevel++
			}
		case '[':
			if nestedTo == '\\' {
				nestedTo = ']'
				nestingLevel = 1
			} else if nestedTo == ']' {
				nestingLevel++
			}
		default:
			if s[nn] == nestedTo {
				nestingLevel--
				if nestingLevel == 0 {
					nestedTo = '\\'
				}
			} else if s[nn] == ':' && nestedTo == '\\' {
				return strings.TrimSpace(s[:nn]), strings.TrimSpace(s[nn+1:])
			}
		}
		nn++
	}
	return strings.TrimSpace(s[:nn]), ""
}

func getNextChain(s string) (chain, left string, tp chainType) {
	s = strings.TrimSpace(s)
	if len(s) < 2 {
		return s, "", ctNone
	}
	var (
		terminator uint8
		levelChar  = s[0]
	)
	switch s[0] {
	case '(':
		terminator = ')'
		tp = ctArray
	case '{':
		terminator = '}'
		tp = ctObject
	case '[':
		terminator = ']'
		tp = ctData
	}
	var (
		nn          = 1
		nestedLevel = 1
	)
	for {
		if s[nn] == levelChar {
			nestedLevel++
		}
		if s[nn] == terminator {
			nestedLevel--
			if nestedLevel == 0 {
				return strings.TrimSpace(s[1:nn]), strings.TrimSpace(s[nn+1:]), tp
			}
		}
		nn++
		if nn+1 > len(s) {
			if nestedLevel > 0 {
				panic("cannot find closing bracket '" + string(terminator) + "'")
			}
			return strings.TrimSpace(s), "", tp
		}
	}
}

/*
({FUNCEXPR :funcid 870 :funcresulttype 25 :funcretset false :funcvariadic false :funcformat 0 :funccollid 100 :inputcollid 100 :args ({RELABELTYPE :arg {VAR :varno 1 :varattno 6 :vartype 16410 :vartypmod -1 :varcollid 100 :varlevelsup 0 :varnoold 1 :varoattno 6 :location 93} :resulttype 25 :resulttypmod -1 :resultcollid 100 :relabelformat 1 :location 101}) :location 87} {FUNCEXPR :funcid 870 :funcresulttype 25 :funcretset false :funcvariadic false :funcformat 0 :funccollid 100 :inputcollid 100 :args ({FUNCEXPR :funcid 3060 :funcresulttype 25 :funcretset false :funcvariadic false :funcformat 0 :funccollid 100 :inputcollid 100 :args ({RELABELTYPE :arg {VAR :varno 1 :varattno 9 :vartype 1043 :vartypmod 68 :varcollid 100 :varlevelsup 0 :varnoold 1 :varoattno 9 :location 121} :resulttype 25 :resulttypmod -1 :resultcollid 100 :relabelformat 1 :location 126} {CONST :consttype 23 :consttypmod -1 :constcollid 0 :constlen 4 :constbyval true :constisnull false :location 134 :constvalue 4 [ 5 0 0 0 0 0 0 0 ]}) :location 116}) :location 110})
({FUNCEXPR :funcid 870 :funcresulttype 25 :funcretset false :funcvariadic false :funcformat 0 :funccollid 100 :inputcollid 100 :args ({OPEXPR :opno 654 :opfuncid 1258 :opresulttype 25 :opretset false :opcollid 100 :inputcollid 100 :args ({RELABELTYPE :arg {VAR :varno 1 :varattno 6 :vartype 16410 :vartypmod -1 :varcollid 100 :varlevelsup 0 :varnoold 1 :varoattno 6 :location 93} :resulttype 25 :resulttypmod -1 :resultcollid 100 :relabelformat 1 :location 101} {CONST :consttype 25 :consttypmod -1 :constcollid 100 :constlen -1 :constbyval false :constisnull false :location 111 :constvalue 5 [ 20 0 0 0 95 ]}) :location 108}) :location 87} {FUNCEXPR :funcid 870 :funcresulttype 25 :funcretset false :funcvariadic false :funcformat 0 :funccollid 100 :inputcollid 100 :args ({FUNCEXPR :funcid 3060 :funcresulttype 25 :funcretset false :funcvariadic false :funcformat 0 :funccollid 100 :inputcollid 100 :args ({RELABELTYPE :arg {VAR :varno 1 :varattno 9 :vartype 1043 :vartypmod 68 :varcollid 100 :varlevelsup 0 :varnoold 1 :varoattno 9 :location 128} :resulttype 25 :resulttypmod -1 :resultcollid 100 :relabelformat 1 :location 133} {CONST :consttype 23 :consttypmod -1 :constcollid 0 :constlen 4 :constbyval true :constisnull false :location 141 :constvalue 4 [ 5 0 0 0 0 0 0 0 ]}) :location 123}) :location 117})
({FUNCEXPR :funcid 870 :funcresulttype 25 :funcretset false :funcvariadic false :funcformat 0 :funccollid 100 :inputcollid 100 :args ({OPEXPR :opno 654 :opfuncid 1258 :opresulttype 25 :opretset false :opcollid 100 :inputcollid 100 :args ({RELABELTYPE :arg {VAR :varno 1 :varattno 6 :vartype 16410 :vartypmod -1 :varcollid 100 :varlevelsup 0 :varnoold 1 :varoattno 6 :location 93} :resulttype 25 :resulttypmod -1 :resultcollid 100 :relabelformat 1 :location 101} {CONST :consttype 25 :consttypmod -1 :constcollid 100 :constlen -1 :constbyval false :constisnull false :location 111 :constvalue 7 [ 28 0 0 0 95 125 41 ]}) :location 108}) :location 87} {FUNCEXPR :funcid 870 :funcresulttype 25 :funcretset false :funcvariadic false :funcformat 0 :funccollid 100 :inputcollid 100 :args ({FUNCEXPR :funcid 3060 :funcresulttype 25 :funcretset false :funcvariadic false :funcformat 0 :funccollid 100 :inputcollid 100 :args ({RELABELTYPE :arg {VAR :varno 1 :varattno 9 :vartype 1043 :vartypmod 68 :varcollid 100 :varlevelsup 0 :varnoold 1 :varoattno 9 :location 130} :resulttype 25 :resulttypmod -1 :resultcollid 100 :relabelformat 1 :location 135} {CONST :consttype 23 :consttypmod -1 :constcollid 0 :constlen 4 :constbyval true :constisnull false :location 143 :constvalue 4 [ 5 0 0 0 0 0 0 0 ]}) :location 125}) :location 119})
*/
