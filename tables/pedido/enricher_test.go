package pedido

import (
	"reflect"
	"testing"
)

func TestParseOpcoesIDs(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want []int64
	}{
		{"vazio", "", nil},
		{"colchetes vazios", "[]", nil},
		{"um id", "[2]", []int64{2}},
		{"múltiplos ids", "[1,2,3]", []int64{1, 2, 3}},
		{"com espaços", "[1, 2, 3]", []int64{1, 2, 3}},
		{"valor inválido ignorado", "[1,abc,3]", []int64{1, 3}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseOpcoesIDs(tc.raw)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("parseOpcoesIDs(%q) = %v, want %v", tc.raw, got, tc.want)
			}
		})
	}
}

func TestParseNumeroBR(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want float64
	}{
		{"vazio", "", 0},
		{"inteiro", "42", 42},
		{"milhar e decimal", "1.234,56", 1234.56},
		{"apenas decimal", "0,5", 0.5},
		{"inválido", "abc", 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseNumeroBR(tc.raw); got != tc.want {
				t.Errorf("parseNumeroBR(%q) = %v, want %v", tc.raw, got, tc.want)
			}
		})
	}
}

func TestFormatCampoPersonalizadoValor(t *testing.T) {
	cases := []struct {
		name    string
		tipo    int
		valor   string
		titulos []string
		want    interface{}
	}{
		{"texto", campoTipoTexto, "abc", nil, "abc"},
		{"numérico", campoTipoNumerico, "1.234,56", nil, 1234.56},
		{"toggle ligado", campoTipoToggle, "1", nil, 1},
		{"toggle desligado", campoTipoToggle, "", nil, 0},
		{"box múltipla", campoTipoBoxMultipla, "", []string{"Sim", "Não"}, []string{"Sim", "Não"}},
		{"box múltipla sem opções", campoTipoBoxMultipla, "", nil, []string{}},
		{"box única", campoTipoBoxUnica, "", []string{"Sim"}, "Sim"},
		{"box única sem opções", campoTipoBoxUnica, "", nil, ""},
		{"data", campoTipoData, "2026-05-29", nil, "2026-05-29"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := formatCampoPersonalizadoValor(tc.tipo, tc.valor, tc.titulos)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("formatCampoPersonalizadoValor(%d, %q, %v) = %v, want %v", tc.tipo, tc.valor, tc.titulos, got, tc.want)
			}
		})
	}
}
