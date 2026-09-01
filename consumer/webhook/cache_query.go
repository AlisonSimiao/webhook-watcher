package webhook

import (
	"context"
	"database/sql"
)

// hookRow é uma linha do resultado da query de resolução de destino
// (tenant/tipo/url), como configurada em hub_config.hook_query.
type hookRow struct {
	Tenant string
	Tipo   string
	URL    string
}

// queryHookDestinations roda query (vinda de config.LoadHubConfig) contra db
// e escaneia o resultado. A query precisa devolver exatamente 3 colunas,
// nessa ordem: tenant, tipo, url — se a query configurada tiver uma forma
// diferente, o erro aparece aqui (contagem errada) ou, pior, os dados vêm
// errados sem erro (ordem trocada). O comando "hub query" existe para pegar
// isso antes de virar um incidente em produção.
func queryHookDestinations(ctx context.Context, db *sql.DB, query string) ([]hookRow, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []hookRow
	for rows.Next() {
		var r hookRow
		if err := rows.Scan(&r.Tenant, &r.Tipo, &r.URL); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
