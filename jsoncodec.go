package columnstore

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

func jsonMarshal(v any) ([]byte, error) { return json.Marshal(v) }

func jsonUnmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

// typedJSONValue 把缺省值转成能在 JSON 中精确保存的形态。
// 标量类型由解码端依据列类型还原；[]byte 用 base64 字符串承载。
func typedJSONValue(t ColumnType, v any) any {
	if v == nil {
		return nil
	}
	if t == TypeBytes {
		return base64.StdEncoding.EncodeToString(v.([]byte))
	}
	return v
}

// decodeTypedJSONValue 按列类型把 JSON 解码出的通用值还原为精确 Go 类型。
func decodeTypedJSONValue(t ColumnType, v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch t {
	case TypeInt64:
		f, ok := v.(float64) // encoding/json 对数字默认给 float64
		if !ok {
			return nil, fmt.Errorf("expected json number for int64 default, got %T", v)
		}
		return int64(f), nil
	case TypeFloat64:
		f, ok := v.(float64)
		if !ok {
			return nil, fmt.Errorf("expected json number for float64 default, got %T", v)
		}
		return f, nil
	case TypeString:
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("expected json string, got %T", v)
		}
		return s, nil
	case TypeBool:
		b, ok := v.(bool)
		if !ok {
			return nil, fmt.Errorf("expected json bool, got %T", v)
		}
		return b, nil
	case TypeBytes:
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("expected base64 string for bytes, got %T", v)
		}
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, fmt.Errorf("decode base64 bytes default: %w", err)
		}
		return b, nil
	default:
		return nil, fmt.Errorf("unknown column type %s", t)
	}
}
