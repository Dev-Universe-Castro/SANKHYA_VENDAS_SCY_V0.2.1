import oracledb from 'oracledb';
import { getOracleConnection } from './oracle-service';
import { obterToken } from './sankhya-api';

/**
 * Função auxiliar para buscar contrato ativo
 */
async function buscarContratoAtivo() {
    try {
        const { listarContratos } = await import('./oracle-service');
        const contratos = await listarContratos();
        return contratos.find((c: any) => c.ATIVO === true);
    } catch (error) {
        console.error("Erro ao buscar contrato ativo:", error);
        return null;
    }
}

interface EstadoSankhya {
    CODUF: number;
    UF: string;
    DESCRICAO: string;
}

interface SyncResult {
    success: boolean;
    idSistema: number;
    empresa: string;
    totalRegistros: number;
    registrosInseridos: number;
    registrosAtualizados: number;
    registrosDeletados: number;
    dataInicio: Date;
    dataFim: Date;
    duracao: number;
    erro?: string;
}

/**
 * Busca todos os estados do Sankhya
 */
async function buscarEstadosSankhya(idSistema: number, bearerToken: string): Promise<EstadoSankhya[]> {
    console.log(`🔍 [Sync] Buscando Estados (UF) do Sankhya para ID_SISTEMA: ${idSistema}`);

    let allEstados: EstadoSankhya[] = [];
    let currentPage = 0;
    let hasMoreData = true;

    let currentToken = bearerToken;

    while (hasMoreData) {
        console.log(`📄 [Sync] Buscando página ${currentPage} de estados...`);

        const PAYLOAD = {
            "requestBody": {
                "dataSet": {
                    "rootEntity": "UnidadeFederativa",
                    "includePresentationFields": "N",
                    "useFileBasedPagination": true,
                    "disableRowsLimit": true,
                    "offsetPage": currentPage.toString(),
                    "entity": {
                        "fieldset": {
                            "list": "CODUF, UF, DESCRICAO"
                        }
                    }
                }
            }
        };

        const contratoAtivo = await buscarContratoAtivo();
        if (!contratoAtivo) {
            throw new Error("Nenhum contrato ativo encontrado");
        }
        const isSandbox = contratoAtivo.IS_SANDBOX === true;
        const baseUrl = isSandbox
            ? "https://api.sandbox.sankhya.com.br"
            : "https://api.sankhya.com.br";
        const URL_CONSULTA_ATUAL = `${baseUrl}/gateway/v1/mge/service.sbr?serviceName=CRUDServiceProvider.loadRecords&outputType=json`;

        const axios = require('axios');

        try {
            const response = await axios.post(URL_CONSULTA_ATUAL, PAYLOAD, {
                headers: {
                    'Authorization': `Bearer ${currentToken}`,
                    'Content-Type': 'application/json'
                },
                timeout: 60000
            });

            const respostaCompleta = response.data;

            if (respostaCompleta.status === '0') {
                // Sucesso
            } else if (respostaCompleta.statusMessage) {
                console.error(`❌ [Sync] Erro na API Sankhya: ${respostaCompleta.statusMessage}`);
            }

            const entities = respostaCompleta.responseBody?.entities;

            if (!entities || !entities.entity) {
                console.log(`⚠️ [Sync] Nenhum estado encontrado na página ${currentPage}`);
                break;
            }

            const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
            const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];

            const estadosPagina = entityArray.map((rawEntity: any) => {
                const cleanObject: any = {};
                for (let i = 0; i < fieldNames.length; i++) {
                    const fieldKey = `f${i}`;
                    const fieldName = fieldNames[i];
                    if (rawEntity[fieldKey]) {
                        cleanObject[fieldName] = rawEntity[fieldKey].$;
                    }
                }
                return cleanObject as EstadoSankhya;
            });

            allEstados = allEstados.concat(estadosPagina);
            console.log(`✅ [Sync] Página ${currentPage}: ${estadosPagina.length} registros (total acumulado: ${allEstados.length})`);

            const hasMoreResult = entities.hasMoreResult === 'true' || entities.hasMoreResult === true;

            if (estadosPagina.length === 0 || !hasMoreResult) {
                hasMoreData = false;
                console.log(`🏁 [Sync] Última página atingida`);
            } else {
                currentPage++;
                await new Promise(resolve => setTimeout(resolve, 500));
            }
        } catch (error: any) {
            if (error.response?.status === 401 || error.response?.status === 403) {
                console.log(`🔄 [Sync] Token expirado na página ${currentPage}, renovando...`);
                currentToken = await obterToken(idSistema, true);
                console.log(`✅ [Sync] Novo token obtido, continuando da página ${currentPage}`);
                await new Promise(resolve => setTimeout(resolve, 1000));
            } else {
                console.error(`❌ [Sync] Erro fatal na página ${currentPage}:`, error.message);
                throw error;
            }
        }
    }

    console.log(`✅ [Sync] Total de ${allEstados.length} estados recuperados`);
    return allEstados;
}

/**
 * Marca todos como não atuais (soft delete)
 */
async function marcarTodosComoNaoAtuais(connection: oracledb.Connection, idSistema: number): Promise<number> {
    const result = await connection.execute(
        `UPDATE AS_ESTADOS 
     SET SANKHYA_ATUAL = 'N', 
         DT_ULT_CARGA = CURRENT_TIMESTAMP 
     WHERE ID_SISTEMA = :idSistema 
       AND SANKHYA_ATUAL = 'S'`,
        { idSistema },
        { autoCommit: false }
    );

    const rowsAffected = result.rowsAffected || 0;
    return rowsAffected;
}

/**
 * Executa UPSERT de estados
 */
async function upsertEstados(
    connection: oracledb.Connection,
    idSistema: number,
    estados: EstadoSankhya[]
): Promise<{ inseridos: number; atualizados: number }> {
    let inseridos = 0;
    let atualizados = 0;

    const BATCH_SIZE = 100;

    for (let i = 0; i < estados.length; i += BATCH_SIZE) {
        const batch = estados.slice(i, i + BATCH_SIZE);

        for (const estado of batch) {
            const result = await connection.execute(
                `MERGE INTO AS_ESTADOS dest
         USING (SELECT :idSistema AS ID_SISTEMA, :codUf AS CODUF FROM DUAL) src
         ON (dest.ID_SISTEMA = src.ID_SISTEMA AND dest.CODUF = src.CODUF)
         WHEN MATCHED THEN
           UPDATE SET
             UF = :uf,
             DESCRICAO = :descricao,
             SANKHYA_ATUAL = 'S',
             DT_ULT_CARGA = CURRENT_TIMESTAMP
         WHEN NOT MATCHED THEN
           INSERT (
             ID_SISTEMA, CODUF, UF, DESCRICAO,
             SANKHYA_ATUAL, DT_ULT_CARGA, DT_CRIACAO
           )
           VALUES (
             :idSistema, :codUf, :uf, :descricao,
             'S', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
           )`,
                {
                    idSistema,
                    codUf: estado.CODUF,
                    uf: estado.UF,
                    descricao: estado.DESCRICAO
                },
                { autoCommit: false }
            );

            if (result.rowsAffected && result.rowsAffected > 0) {
                const checkResult = await connection.execute(
                    `SELECT DT_CRIACAO FROM AS_ESTADOS 
            WHERE ID_SISTEMA = :idSistema AND CODUF = :codUf`,
                    { idSistema, codUf: estado.CODUF },
                    { outFormat: oracledb.OUT_FORMAT_OBJECT }
                );

                if (checkResult.rows && checkResult.rows.length > 0) {
                    const row: any = checkResult.rows[0];
                    const dtCriacao = new Date(row.DT_CRIACAO);
                    const agora = new Date();
                    const diferencaMs = agora.getTime() - dtCriacao.getTime();

                    if (diferencaMs < 5000) {
                        inseridos++;
                    } else {
                        atualizados++;
                    }
                }
            }
        }

        await connection.commit();
    }

    return { inseridos, atualizados };
}

/**
 * Sincroniza estados de uma empresa específica
 */
export async function sincronizarEstadosPorEmpresa(idSistema: number, empresaNome: string): Promise<SyncResult> {
    const dataInicio = new Date();
    let connection: oracledb.Connection | undefined;

    try {
        console.log(`🚀 SINCRONIZAÇÃO DE ESTADOS para ${empresaNome} (ID: ${idSistema})`);

        let bearerToken = await obterToken(idSistema, true);
        const estados = await buscarEstadosSankhya(idSistema, bearerToken);
        connection = await getOracleConnection();

        const registrosDeletados = await marcarTodosComoNaoAtuais(connection, idSistema);
        const { inseridos, atualizados } = await upsertEstados(connection, idSistema, estados);

        await connection.commit();

        const dataFim = new Date();
        const duracao = dataFim.getTime() - dataInicio.getTime();

        try {
            const { salvarLogSincronizacao } = await import('./sync-logs-service');
            await salvarLogSincronizacao({
                ID_SISTEMA: idSistema,
                EMPRESA: empresaNome,
                TABELA: 'AS_ESTADOS',
                STATUS: 'SUCESSO',
                TOTAL_REGISTROS: estados.length,
                REGISTROS_INSERIDOS: inseridos,
                REGISTROS_ATUALIZADOS: atualizados,
                REGISTROS_DELETADOS: registrosDeletados,
                DURACAO_MS: duracao,
                DATA_INICIO: dataInicio,
                DATA_FIM: dataFim
            });
        } catch (logError) {
            console.error('❌ [Sync] Erro ao salvar log:', logError);
        }

        return {
            success: true,
            idSistema,
            empresa: empresaNome,
            totalRegistros: estados.length,
            registrosInseridos: inseridos,
            registrosAtualizados: atualizados,
            registrosDeletados,
            dataInicio,
            dataFim,
            duracao
        };

    } catch (error: any) {
        console.error(`❌ [Sync] Erro ao sincronizar estados para ${empresaNome}:`, error);

        if (connection) {
            try { await connection.rollback(); } catch { }
        }

        const dataFim = new Date();
        const duracao = dataFim.getTime() - dataInicio.getTime();

        try {
            const { salvarLogSincronizacao } = await import('./sync-logs-service');
            await salvarLogSincronizacao({
                ID_SISTEMA: idSistema,
                EMPRESA: empresaNome,
                TABELA: 'AS_ESTADOS',
                STATUS: 'FALHA',
                TOTAL_REGISTROS: 0,
                REGISTROS_INSERIDOS: 0,
                REGISTROS_ATUALIZADOS: 0,
                REGISTROS_DELETADOS: 0,
                DURACAO_MS: duracao,
                MENSAGEM_ERRO: error.message,
                DATA_INICIO: dataInicio,
                DATA_FIM: dataFim
            });
        } catch { }

        return {
            success: false,
            idSistema,
            empresa: empresaNome,
            totalRegistros: 0,
            registrosInseridos: 0,
            registrosAtualizados: 0,
            registrosDeletados: 0,
            dataInicio,
            dataFim,
            duracao,
            erro: error.message
        };

    } finally {
        if (connection) {
            try { await connection.close(); } catch { }
        }
    }
}

/**
 * Sincroniza todas as empresas
 */
export async function sincronizarTodosEstados(): Promise<SyncResult[]> {
    let connection: oracledb.Connection | undefined;
    const resultados: SyncResult[] = [];

    try {
        connection = await getOracleConnection();
        const result = await connection.execute(
            `SELECT ID_EMPRESA, EMPRESA FROM AD_CONTRATOS WHERE ATIVO = 'S' ORDER BY EMPRESA`,
            [],
            { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );
        await connection.close();
        connection = undefined;

        const empresas = result.rows as any[];

        for (const empresa of empresas) {
            const resultado = await sincronizarEstadosPorEmpresa(empresa.ID_EMPRESA, empresa.EMPRESA);
            resultados.push(resultado);
            await new Promise(resolve => setTimeout(resolve, 2000));
        }

        return resultados;

    } catch (error: any) {
        throw error;
    } finally {
        if (connection) { try { await connection.close(); } catch { } }
    }
}

/**
 * Estatísticas
 */
export async function obterEstatisticasSincronizacao(idSistema?: number) {
    const connection = await getOracleConnection();

    try {
        const whereClause = idSistema ? `WHERE ID_SISTEMA = :idSistema` : '';
        const params = idSistema ? { idSistema } : {};

        const result = await connection.execute(
            `SELECT 
         ID_SISTEMA,
         COUNT(*) as TOTAL_REGISTROS,
         SUM(CASE WHEN SANKHYA_ATUAL = 'S' THEN 1 ELSE 0 END) as REGISTROS_ATIVOS,
         SUM(CASE WHEN SANKHYA_ATUAL = 'N' THEN 1 ELSE 0 END) as REGISTROS_DELETADOS,
         MAX(DT_ULT_CARGA) as ULTIMA_SINCRONIZACAO
       FROM AS_ESTADOS
       ${whereClause}
       GROUP BY ID_SISTEMA
       ORDER BY ID_SISTEMA`,
            params,
            { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );

        return result.rows || [];
    } finally {
        await connection.close();
    }
}

/**
 * Listar estados sincronizados
 */
export async function listarEstados(idSistema?: number) {
    const connection = await getOracleConnection();

    try {
        const whereClause = idSistema ? `WHERE E.ID_SISTEMA = :idSistema` : '';
        const params = idSistema ? { idSistema } : {};

        const result = await connection.execute(
            `SELECT 
                E.ID_SISTEMA,
                C.EMPRESA as NOME_CONTRATO,
                E.CODUF,
                E.UF,
                E.DESCRICAO,
                E.SANKHYA_ATUAL,
                E.DT_ULT_CARGA
            FROM AS_ESTADOS E
            JOIN AD_CONTRATOS C ON C.ID_EMPRESA = E.ID_SISTEMA
            ${whereClause}
            ORDER BY E.ID_SISTEMA, E.UF`,
            params,
            { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );

        return result.rows || [];
    } finally {
        await connection.close();
    }
}
