package jsonlogic2sql

func mustNewSchema(fields []FieldSchema) *Schema {
	schema, err := NewValidatedSchema(fields)
	if err != nil {
		panic(err)
	}
	return schema
}
