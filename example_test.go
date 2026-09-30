package columnstore_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	columnstore "github.com/h3yow/columnar-local-storage-go"
)

func Example() {
	dir, err := os.MkdirTemp("", "cs-example-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	schema, err := columnstore.NewSchema([]columnstore.Column{
		{Name: "id", Type: columnstore.TypeInt64, Nullable: false},
		{Name: "name", Type: columnstore.TypeString, Nullable: true},
	})
	if err != nil {
		log.Fatal(err)
	}
	st, err := columnstore.Open(dir, schema, &columnstore.Options{
		MaxSegments: 2,
		Logger:      log.New(io.Discard, "", 0),
	})
	if err != nil {
		log.Fatal(err)
	}

	_ = st.Write(columnstore.Batch{
		{"id": int64(1), "name": "alice"},
		{"id": int64(2), "name": nil},
	})
	// 非法记录整批拒绝，错误原因可区分。
	err = st.Write(columnstore.Batch{{"id": "not-int", "name": "x"}})
	fmt.Println("bad write:", errors.Is(err, columnstore.ErrTypeMismatch))

	// 加列：旧行按缺省（此处为 null）读回。
	if err := st.AddColumn("tag", columnstore.TypeString, true, nil); err != nil {
		log.Fatal(err)
	}
	_ = st.Write(columnstore.Batch{{"id": int64(3), "name": "carol", "tag": "new"}})

	if err := st.ForceCompaction(context.Background()); err != nil {
		log.Fatal(err)
	}

	rs, err := st.ReadColumns([]string{"id", "name", "tag"})
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < rs.Rows; i++ {
		name := "NULL"
		if !rs.Columns[1].Nulls[i] {
			name = rs.Columns[1].Values[i].(string)
		}
		tag := "NULL"
		if !rs.Columns[2].Nulls[i] {
			tag = rs.Columns[2].Values[i].(string)
		}
		fmt.Printf("row %d: id=%d name=%s tag=%s\n", i, rs.Columns[0].Values[i], name, tag)
	}
	// Output:
	// bad write: true
	// row 0: id=1 name=alice tag=NULL
	// row 1: id=2 name=NULL tag=NULL
	// row 2: id=3 name=carol tag=new
}
