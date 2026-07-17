package main

import (
	"archive/zip"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/text/encoding/charmap"
)

var (
	dataDir   = "/data"
	batchSize = 20000

	porteMap = map[string]string{
		"00": "NÃO INFORMADO",
		"01": "MICRO EMPRESA",
		"03": "EMPRESA DE PEQUENO PORTE",
		"05": "DEMAIS",
	}

	njGrupoMap = map[string]string{
		"1": "ADMINISTRAÇÃO PÚBLICA",
		"2": "ENTIDADES EMPRESARIAIS",
		"3": "ENTIDADES SEM FINS LUCRATIVOS",
		"4": "PESSOAS FÍSICAS",
		"5": "ORGANIZAÇÕES INTERNACIONAIS",
	}

	nonDigitRegex = regexp.MustCompile(`\D`)
	meiRegex      = regexp.MustCompile(`\d{11}$`)
)

func getCapitalFaixa(capital float64) string {
	if capital <= 0 {
		return "SEM CAPITAL"
	}
	if capital <= 1000 {
		return "ATÉ 1K"
	}
	if capital <= 10000 {
		return "1K A 10K"
	}
	if capital <= 100000 {
		return "10K A 100K"
	}
	if capital <= 1000000 {
		return "100K A 1M"
	}
	return "ACIMA DE 1M"
}

func getNaturezaGrupo(natureza string) string {
	if len(natureza) == 0 {
		return "OUTROS"
	}
	primeiroDigito := string(natureza[0])
	if grupo, ok := njGrupoMap[primeiroDigito]; ok {
		return grupo
	}
	return "OUTROS"
}

func isMei(razao string) bool {
	return meiRegex.MatchString(razao)
}

func parseCapital(raw string) float64 {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0.0
	}
	s = strings.ReplaceAll(s, ".", "")
	s = strings.ReplaceAll(s, ",", ".")
	val, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0.0
	}
	return val
}

func parseNatureza(raw string) string {
	s := nonDigitRegex.ReplaceAllString(raw, "")
	if len(s) > 4 {
		s = s[len(s)-4:]
	}
	for len(s) < 4 {
		s = "0" + s
	}
	return s
}

func parseCnpj(raw string) string {
	s := nonDigitRegex.ReplaceAllString(raw, "")
	if len(s) > 8 {
		s = s[len(s)-8:]
	}
	for len(s) < 8 {
		s = "0" + s
	}
	return s
}

func parsePorte(raw string) string {
	s := nonDigitRegex.ReplaceAllString(raw, "")
	if len(s) > 2 {
		s = s[len(s)-2:]
	}
	for len(s) < 2 {
		s = "0" + s
	}
	if _, ok := porteMap[s]; !ok {
		return "00"
	}
	return s
}

func main() {
	participante := os.Getenv("PARTICIPANTE")
	if participante == "" {
		log.Fatal("Variável PARTICIPANTE não definida")
	}
	pgTable := os.Getenv("PG_TABLE")
	if pgTable == "" {
		pgTable = fmt.Sprintf("%s_empresas", participante)
	}

	pgHost := os.Getenv("PG_HOST")
	if pgHost == "" {
		pgHost = "postgres_db"
	}
	pgPort := os.Getenv("PG_PORT")
	if pgPort == "" {
		pgPort = "5432"
	}
	pgUser := os.Getenv("PG_USER")
	pgPassword := os.Getenv("PG_PASSWORD")
	pgDB := os.Getenv("PG_DB")
	if pgDB == "" {
		pgDB = "db_empresas"
	}

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", pgUser, pgPassword, pgHost, pgPort, pgDB)

	ctx := context.Background()

	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		log.Fatalf("Erro ao fazer parse da DSN: %v", err)
	}
	// Limit max connections to prevent high memory usage
	poolConfig.MaxConns = 10

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		log.Fatalf("Erro ao conectar ao banco: %v", err)
	}
	defer pool.Close()

	log.Printf("=== Ingestão no Limite (Go) ===")
	log.Printf("Participante : %s", participante)
	log.Printf("Tabela destino: public.%s", pgTable)
	log.Printf("Postgres     : %s@%s:%s/%s", pgUser, pgHost, pgPort, pgDB)

	// Prepare table
	ddl := fmt.Sprintf(`
	DROP TABLE IF EXISTS public."%s";
	CREATE TABLE public."%s" (
		cnpj_basico                  VARCHAR(8) NOT NULL,
		razao_social                 VARCHAR NOT NULL,
		natureza_juridica             VARCHAR(4) NOT NULL,
		qualificacao_responsavel      VARCHAR NOT NULL,
		capital_social               DOUBLE PRECISION NOT NULL,
		porte_codigo                 VARCHAR(2) NOT NULL,
		porte_descricao              VARCHAR NOT NULL,
		ente_federativo              VARCHAR,
		capital_social_faixa         VARCHAR NOT NULL,
		is_mei                       BOOLEAN NOT NULL,
		natureza_juridica_grupo      VARCHAR NOT NULL,
		ente_federativo_presente     BOOLEAN NOT NULL,
		data_processamento           TIMESTAMP NOT NULL
	);
	`, pgTable, pgTable)

	_, err = pool.Exec(ctx, ddl)
	if err != nil {
		log.Fatalf("Erro ao criar tabela: %v", err)
	}

	zips, err := filepath.Glob(filepath.Join(dataDir, "*.zip"))
	if err != nil || len(zips) == 0 {
		log.Fatalf("Nenhum arquivo .zip encontrado em %s", dataDir)
	}

	sort.Strings(zips)

	totalInserted := 0
	ts := time.Now().UTC()

	columns := []string{
		"cnpj_basico", "razao_social", "natureza_juridica",
		"qualificacao_responsavel", "capital_social", "porte_codigo",
		"porte_descricao", "ente_federativo", "capital_social_faixa",
		"is_mei", "natureza_juridica_grupo", "ente_federativo_presente",
		"data_processamento",
	}

	seen := make([]uint64, 100000000/64+1)

	for _, zpPath := range zips {
		log.Printf("Processando %s...", filepath.Base(zpPath))
		zr, err := zip.OpenReader(zpPath)
		if err != nil {
			log.Printf("Erro ao abrir zip %s: %v", zpPath, err)
			continue
		}

		for _, f := range zr.File {
			if !strings.HasSuffix(strings.ToUpper(f.Name), ".EMPRECSV") {
				continue
			}

			rc, err := f.Open()
			if err != nil {
				log.Printf("Erro ao abrir arquivo dentro do zip: %v", err)
				continue
			}

			// Decodificador ISO-8859-1
			decoder := charmap.ISO8859_1.NewDecoder()
			reader := decoder.Reader(rc)

			csvReader := csv.NewReader(reader)
			csvReader.Comma = ';'
			csvReader.LazyQuotes = true
			csvReader.FieldsPerRecord = -1

			var batch [][]any

			for {
				record, err := csvReader.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					continue
				}

				if len(record) < 7 {
					continue
				}

				cnpj := parseCnpj(record[0])
				cnpjInt, err := strconv.Atoi(cnpj)
				if err == nil {
					idx := cnpjInt / 64
					bit := uint64(1) << (cnpjInt % 64)
					if (seen[idx] & bit) != 0 {
						continue // skip duplicate
					}
					seen[idx] |= bit
				}

				razao := strings.ToUpper(strings.TrimSpace(record[1]))
				natureza := parseNatureza(record[2])
				qualificacao := strings.TrimSpace(record[3])
				capital := parseCapital(record[4])
				porte := parsePorte(record[5])
				enteRaw := strings.TrimSpace(record[6])

				var ente *string
				if enteRaw != "" {
					ente = &enteRaw
				}

				capitalFaixa := getCapitalFaixa(capital)
				isMeiFlag := isMei(razao)
				njGrupo := getNaturezaGrupo(natureza)
				entePresente := ente != nil
				porteDesc := porteMap[porte]

				row := []any{
					cnpj,
					razao,
					natureza,
					qualificacao,
					capital,
					porte,
					porteDesc,
					ente,
					capitalFaixa,
					isMeiFlag,
					njGrupo,
					entePresente,
					ts,
				}

				batch = append(batch, row)

				if len(batch) >= batchSize {
					_, err := pool.CopyFrom(
						ctx,
						pgx.Identifier{"public", pgTable},
						columns,
						pgx.CopyFromRows(batch),
					)
					if err != nil {
						log.Printf("Erro no CopyFrom: %v", err)
					} else {
						totalInserted += len(batch)
					}
					batch = batch[:0]
				}
			}

			if len(batch) > 0 {
				_, err := pool.CopyFrom(
					ctx,
					pgx.Identifier{"public", pgTable},
					columns,
					pgx.CopyFromRows(batch),
				)
				if err != nil {
					log.Printf("Erro no CopyFrom (final): %v", err)
				} else {
					totalInserted += len(batch)
				}
			}

			rc.Close()
		}
		zr.Close()
	}

	// Criar o índice único ao final (muito mais rápido que ter durante a inserção)
	log.Printf("Criando índice único...")
	idxDDL := fmt.Sprintf(`CREATE UNIQUE INDEX idx_cnpj_unique ON public."%s" (cnpj_basico);`, pgTable)
	_, err = pool.Exec(ctx, idxDDL)
	if err != nil {
		log.Fatalf("Erro ao criar índice único: %v", err)
	}

	log.Printf("Concluído — %d linhas em public.%s", totalInserted, pgTable)
	if totalInserted == 0 {
		os.Exit(1)
	}
}
