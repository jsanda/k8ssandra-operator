package cassandra

import (
	"github.com/Masterminds/semver/v3"
	"github.com/stretchr/testify/assert"
	"k8s.io/utils/pointer"
	"reflect"
	"testing"
)

func Test_preMarshalConfig(t *testing.T) {
	type simple struct {
		Simple1 *string `cass-config:"*:foo/simple1"`
		Simple2 bool    `cass-config:"*:foo/simple2"`
	}
	type komplex struct {
		ManyRestrictions               *string `cass-config:"^3.11.x:foo/many-restrictions-3x,>=4.x:foo/many-restrictions-4x"`
		V3Only                         *int    `cass-config:"^3.11.x:foo/3x-only"`
		V4Only                         *int    `cass-config:">=4.x:foo/4x-only"`
		RetainZero                     bool    `cass-config:"*:foo/retain-zero;retainzero"`
		nonExported                    int
		Ignored                        int
		ChildNoRecurseRetainZeroV4Only *simple `cass-config:">=4.x:child-4x;retainzero"`
		ChildRecurseNoPath             *simple `cass-config:"*:;recurse"`
		ChildRecurse                   *simple `cass-config:"*:foo;recurse"`
	}
	type invalid1 struct {
		Field1 string `cass-config:"invalid tag"`
	}
	type invalid2 struct {
		Field1 *invalid1 `cass-config:"*:;recurse"`
	}
	type conflict1 struct {
		Field1 *int    `cass-config:"*:foo"`
		Field2 *string `cass-config:"*:foo"`
	}
	type conflict2 struct {
		Field1 *int    `cass-config:"*:foo/simple1"`
		Field2 *simple `cass-config:"*:;recurse"`
	}
	type conflict3 struct {
		Field1 *int    `cass-config:"*:foo/simple1"`
		Field2 *simple `cass-config:"*:foo/simple1;recurse"`
	}
	type conflict4 struct {
		Field1 *int    `cass-config:"*:foo/foo/simple1"`
		Field2 *simple `cass-config:"*:foo;recurse"`
	}
	tests := []struct {
		name    string
		val     reflect.Value
		version *semver.Version
		wantOut map[string]interface{}
		wantErr assert.ErrorAssertionFunc
	}{
		{
			"nil simple",
			reflect.ValueOf((*simple)(nil)),
			semver.MustParse("3.11.12"),
			nil,
			assert.NoError,
		},
		{
			"zero simple",
			reflect.ValueOf(&simple{}),
			semver.MustParse("3.11.12"),
			map[string]interface{}{},
			assert.NoError,
		},
		{
			"zero complex 3.11.x",
			reflect.ValueOf(&komplex{}),
			semver.MustParse("3.11.12"),
			map[string]interface{}{"foo": map[string]interface{}{"retain-zero": false}},
			assert.NoError,
		},
		{
			"zero complex 4.x",
			reflect.ValueOf(&komplex{}),
			semver.MustParse("4.0.3"),
			map[string]interface{}{"child-4x": (*simple)(nil), "foo": map[string]interface{}{"retain-zero": false}},
			assert.NoError,
		},
		{
			"simple",
			reflect.ValueOf(&simple{Simple1: pointer.String("foo"), Simple2: true}),
			semver.MustParse("3.11.12"),
			map[string]interface{}{"foo": map[string]interface{}{"simple1": pointer.String("foo"), "simple2": true}},
			assert.NoError,
		},
		{
			"complex 3.11.x",
			reflect.ValueOf(&komplex{
				ManyRestrictions: pointer.String("qix"),
				V3Only:           pointer.Int(123),
				V4Only:           pointer.Int(456),
				RetainZero:       true,
				nonExported:      789,
				Ignored:          1000,
				ChildNoRecurseRetainZeroV4Only: &simple{
					Simple1: pointer.String("foo"),
					Simple2: true,
				},
				ChildRecurseNoPath: &simple{
					Simple1: pointer.String("bar"),
					Simple2: false,
				},
				ChildRecurse: &simple{
					Simple1: pointer.String("qix"),
					Simple2: true,
				},
			}),
			semver.MustParse("3.11.12"),
			map[string]interface{}{
				"foo": map[string]interface{}{
					"many-restrictions-3x": pointer.String("qix"),
					"3x-only":              pointer.Int(123),
					"retain-zero":          true,
					// from ChildRecurseNoPath:
					"simple1": pointer.String("bar"),
					//"simple2" omitted because zero value not retained
					// from ChildRecurse:
					"foo": map[string]interface{}{
						"simple1": pointer.String("qix"),
						"simple2": true,
					},
				},
				// ChildNoRecurseRetainZeroV4Only not included because only valid for 4.x
			},
			assert.NoError,
		},
		{
			"complex 4.x",
			reflect.ValueOf(&komplex{
				ManyRestrictions: pointer.String("qix"),
				V3Only:           pointer.Int(123),
				V4Only:           pointer.Int(456),
				RetainZero:       true,
				nonExported:      789,
				Ignored:          1000,
				ChildNoRecurseRetainZeroV4Only: &simple{
					Simple1: pointer.String("foo"),
					Simple2: true,
				},
				ChildRecurseNoPath: &simple{
					Simple1: pointer.String("bar"),
					Simple2: false,
				},
				ChildRecurse: &simple{
					Simple1: pointer.String("qix"),
					Simple2: true,
				},
			}),
			semver.MustParse("4.1.0"),
			map[string]interface{}{
				"foo": map[string]interface{}{
					"many-restrictions-4x": pointer.String("qix"),
					"4x-only":              pointer.Int(456),
					"retain-zero":          true,
					// from ChildRecurseNoPath:
					"simple1": pointer.String("bar"),
					//"simple2" omitted because zero value not retained
					// from ChildRecurse:
					"foo": map[string]interface{}{
						"simple1": pointer.String("qix"),
						"simple2": true,
					},
				},
				// from ChildNoRecurseRetainZeroV4Only, which wasn't recursed hence was included as is
				"child-4x": &simple{
					Simple1: pointer.String("foo"),
					Simple2: true,
				},
			},
			assert.NoError,
		},
		{
			"child not found",
			reflect.ValueOf(struct {
				Field1 simple `cass-config:"*:foo/bar;recurse"`
			}{Field1: simple{Simple2: true}}),
			semver.MustParse("4.1.0"),
			map[string]interface{}{"foo": map[string]interface{}{"bar": map[string]interface{}{"foo": map[string]interface{}{"simple2": true}}}},
			assert.NoError,
		},
		{
			"not struct",
			reflect.ValueOf("not a struct"),
			semver.MustParse("4.1.0"),
			nil,
			func(t assert.TestingT, err error, _ ...interface{}) bool {
				return assert.Contains(t, err.Error(), "expected struct, got: string")
			},
		},
		{
			"unparseable tag",
			reflect.ValueOf(invalid1{Field1: "foo"}),
			semver.MustParse("4.1.0"),
			nil,
			func(t assert.TestingT, err error, _ ...interface{}) bool {
				msg := err.Error()
				return assert.NotNil(t, err) &&
					assert.Contains(t, msg, "cannot parse cass-config tag on cassandra.invalid1.Field1") &&
					assert.Contains(t, msg, "wrong path entry: 'invalid tag'")
			},
		},
		{
			"error in child recurse",
			reflect.ValueOf(invalid2{Field1: &invalid1{Field1: "foo"}}),
			semver.MustParse("4.1.0"),
			nil,
			func(t assert.TestingT, err error, _ ...interface{}) bool {
				msg := err.Error()
				return assert.NotNil(t, err) &&
					assert.Contains(t, msg, "field cassandra.invalid2.Field1: recurse failed") &&
					assert.Contains(t, msg, "cannot parse cass-config tag on cassandra.invalid1.Field1") &&
					assert.Contains(t, msg, "wrong path entry: 'invalid tag'")
			},
		},
		{
			"key conflict simple",
			reflect.ValueOf(conflict1{
				Field1: pointer.Int(123),
				Field2: pointer.String("foo"),
			}),
			semver.MustParse("4.1.0"),
			nil,
			func(t assert.TestingT, err error, _ ...interface{}) bool {
				msg := err.Error()
				return assert.NotNil(t, err) &&
					assert.Contains(t, msg, "field cassandra.conflict1.Field2: cannot put value") &&
					assert.Contains(t, msg, "key foo already exists")
			},
		},
		{
			"key conflict recursive 1",
			reflect.ValueOf(conflict2{
				Field1: pointer.Int(123),
				Field2: &simple{Simple1: pointer.String("foo")},
			}),
			semver.MustParse("4.1.0"),
			nil,
			func(t assert.TestingT, err error, _ ...interface{}) bool {
				msg := err.Error()
				return assert.NotNil(t, err) &&
					assert.Contains(t, msg, "field cassandra.conflict2.Field2: cannot merge map") &&
					assert.Contains(t, msg, "key simple1 already exists")
			},
		},
		{
			"key conflict recursive 2",
			reflect.ValueOf(conflict3{
				Field1: pointer.Int(123),
				Field2: &simple{Simple1: pointer.String("abc")},
			}),
			semver.MustParse("4.1.0"),
			nil,
			func(t assert.TestingT, err error, _ ...interface{}) bool {
				msg := err.Error()
				return assert.NotNil(t, err) &&
					assert.Contains(t, msg, "field cassandra.conflict3.Field2: cannot merge map") &&
					assert.Contains(t, msg, "path foo/simple1 exists but its value is of type *int")
			},
		},
		{
			"key conflict recursive 3",
			reflect.ValueOf(conflict4{
				Field1: pointer.Int(123),
				Field2: &simple{Simple1: pointer.String("abc")},
			}),
			semver.MustParse("4.1.0"),
			nil,
			func(t assert.TestingT, err error, _ ...interface{}) bool {
				msg := err.Error()
				return assert.NotNil(t, err) &&
					assert.Contains(t, msg, "field cassandra.conflict4.Field2: cannot merge map") &&
					assert.Contains(t, msg, "key simple1 already exists")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotOut, gotErr := preMarshalConfig(tt.val, tt.version)
			assert.Equal(t, tt.wantOut, gotOut)
			tt.wantErr(t, gotErr)
		})
	}
}
