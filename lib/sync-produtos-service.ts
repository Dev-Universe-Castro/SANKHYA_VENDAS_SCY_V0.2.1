import oracledb from 'oracledb';
import { getOracleConnection } from './oracle-service';
import { fazerRequisicaoAutenticada, obterToken } from './sankhya-api';

const URL_CONSULTA_SERVICO = "https://api.sandbox.sankhya.com.br/gateway/v1/mge/service.sbr?serviceName=CRUDServiceProvider.loadRecords&outputType=json";

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

interface ProdutoSankhya {
  CODPROD: string;
  DESCRPROD: string;
  ATIVO: string;
  LOCAL: string;
  MARCA: string;
  CARACTERISTICAS: string;
  UNIDADE: string;
  VLRCOMERC: string;
  CODGRUPOPROD: string;
  CODMARCA: string;
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
 * Busca todos os produtos do Sankhya para uma empresa específica
 */
async function buscarProdutosSankhya(idSistema: number, bearerToken: string): Promise<ProdutoSankhya[]> {
  console.log(`🔍 [Sync] Buscando produtos do Sankhya para ID_SISTEMA: ${idSistema}`);

  let allProdutos: ProdutoSankhya[] = [];
  let currentPage = 0;
  let hasMoreData = true;
  let currentToken = bearerToken;

  while (hasMoreData) {
    console.log(`📄 [Sync] Buscando página ${currentPage} de produtos...`);

    const PAYLOAD = {
      "requestBody": {
        "dataSet": {
          "rootEntity": "Produto",
          "includePresentationFields": "N",
          "useFileBasedPagination": true,
          "disableRowsLimit": true,
          "offsetPage": currentPage.toString(),
          "entity": {
            "fieldset": {
              "list": "CODPROD, DESCRPROD, ATIVO, LOCAL, MARCA, CARACTERISTICAS, UNIDADE, VLRCOMERC, CODGRUPOPROD, CODMARCA"
            }
          }
        }
      }
    };

    // Obter contrato ativo para determinar ambiente (sandbox/produção)
    const contratoAtivo = await buscarContratoAtivo();
    if (!contratoAtivo) {
      throw new Error("Nenhum contrato ativo encontrado");
    }
    const isSandbox = contratoAtivo.IS_SANDBOX === true;
    const baseUrl = isSandbox
      ? "https://api.sandbox.sankhya.com.br"
      : "https://api.sankhya.com.br";
    const URL_CONSULTA_SERVICO = `${baseUrl}/gateway/v1/mge/service.sbr?serviceName=CRUDServiceProvider.loadRecords&outputType=json`;

    const axios = require('axios');

    try {
      const response = await axios.post(URL_CONSULTA_SERVICO, PAYLOAD, {
        headers: {
          'Authorization': `Bearer ${currentToken}`,
          'Content-Type': 'application/json'
        },
        timeout: 60000
      });

      const respostaCompleta = response.data;

      const entities = respostaCompleta.responseBody?.entities;

      if (!entities || !entities.entity) {
        console.log(`⚠️ [Sync] Nenhum produto encontrado na página ${currentPage}`);
        break;
      }

      const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
      const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];

      const produtosPagina = entityArray.map((rawEntity: any) => {
        const cleanObject: any = {};
        for (let i = 0; i < fieldNames.length; i++) {
          const fieldKey = `f${i}`;
          const fieldName = fieldNames[i];
          if (rawEntity[fieldKey]) {
            cleanObject[fieldName] = rawEntity[fieldKey].$;
          }
        }
        return cleanObject as ProdutoSankhya;
      });

      allProdutos = allProdutos.concat(produtosPagina);
      console.log(`✅ [Sync] Página ${currentPage}: ${produtosPagina.length} registros (total acumulado: ${allProdutos.length})`);

      const hasMoreResult = entities.hasMoreResult === 'true' || entities.hasMoreResult === true;

      if (produtosPagina.length === 0 || !hasMoreResult) {
        hasMoreData = false;
        console.log(`🏁 [Sync] Última página atingida (hasMoreResult: ${hasMoreResult}, registros: ${produtosPagina.length})`);
      } else {
        currentPage++;
        await new Promise(resolve => setTimeout(resolve, 500));
      }
    } catch (error: any) {
      // Se token expirou (401/403), renovar e tentar novamente a mesma página
      if (error.response?.status === 401 || error.response?.status === 403) {
        console.log(`🔄 [Sync] Token expirado na página ${currentPage}, renovando...`);
        console.log(`📊 [Sync] Progresso mantido: ${allProdutos.length} registros acumulados`);
        currentToken = await obterToken(idSistema, true);
        console.log(`✅ [Sync] Novo token obtido, continuando da página ${currentPage}`);
        // Não incrementar currentPage, vai tentar novamente a mesma página
        await new Promise(resolve => setTimeout(resolve, 1000));
      } else {
        console.error(`❌ [Sync] Erro fatal na página ${currentPage}:`, error.message);
        throw error;
      }
    }
  }

  console.log(`✅ [Sync] Total de ${allProdutos.length} produtos recuperados em ${currentPage} páginas`);
  return allProdutos;
}

/**
 * Executa o soft delete (marca como não atual) todos os produtos do sistema
 */
async function marcarTodosComoNaoAtuais(connection: oracledb.Connection, idSistema: number): Promise<number> {
  const result = await connection.execute(
    `UPDATE AS_PRODUTOS
     SET SANKHYA_ATUAL = 'N',
         DT_ULT_CARGA = CURRENT_TIMESTAMP
     WHERE ID_SISTEMA = :idSistema
       AND SANKHYA_ATUAL = 'S'`,
    { idSistema },
    { autoCommit: false }
  );

  const rowsAffected = result.rowsAffected || 0;
  console.log(`🗑️ [Sync] ${rowsAffected} registros marcados como não atuais`);
  return rowsAffected;
}

/**
 * Executa UPSERT de produtos usando MERGE
 */
async function upsertProdutos(
  connection: oracledb.Connection,
  idSistema: number,
  produtos: ProdutoSankhya[],
  bearerToken: string
): Promise<{ inseridos: number; atualizados: number }> {
  let inseridos = 0;
  let atualizados = 0;

  const BATCH_SIZE = 100;

  for (let i = 0; i < produtos.length; i += BATCH_SIZE) {
    const batch = produtos.slice(i, i + BATCH_SIZE);

    for (const produto of batch) {
      const result = await connection.execute(
        `MERGE INTO AS_PRODUTOS dest
         USING (SELECT :idSistema AS ID_SISTEMA, :codProd AS CODPROD FROM DUAL) src
         ON (dest.ID_SISTEMA = src.ID_SISTEMA AND dest.CODPROD = src.CODPROD)
         WHEN MATCHED THEN
           UPDATE SET
             DESCRPROD = :descrProd,
             ATIVO = :ativo,
             LOCAL = :local,
             MARCA = :marca,
             CARACTERISTICAS = :caracteristicas,
             UNIDADE = :unidade,
             VLRCOMERC = :vlrComerc,
             CODGRUPOPROD = :codGrupoProd,
             CODMARCA = :codMarca,
             SANKHYA_ATUAL = 'S',
             DT_ULT_CARGA = CURRENT_TIMESTAMP
         WHEN NOT MATCHED THEN
           INSERT (
             ID_SISTEMA, CODPROD, DESCRPROD, ATIVO, LOCAL, MARCA, CARACTERISTICAS,
             UNIDADE, VLRCOMERC, CODGRUPOPROD, CODMARCA, SANKHYA_ATUAL, DT_ULT_CARGA, DT_CRIACAO
           )
           VALUES (
             :idSistema, :codProd, :descrProd, :ativo, :local, :marca, :caracteristicas,
             :unidade, :vlrComerc, :codGrupoProd, :codMarca, 'S', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
           )`,
        {
          idSistema,
          codProd: produto.CODPROD || null,
          descrProd: produto.DESCRPROD || null,
          ativo: produto.ATIVO || null,
          local: produto.LOCAL || null,
          marca: produto.MARCA || null,
          caracteristicas: produto.CARACTERISTICAS || null,
          unidade: produto.UNIDADE || null,
          vlrComerc: produto.VLRCOMERC || null,
          codGrupoProd: produto.CODGRUPOPROD || null,
          codMarca: produto.CODMARCA || null
        },
        { autoCommit: false }
      );

      if (result.rowsAffected && result.rowsAffected > 0) {
        const checkResult = await connection.execute(
          `SELECT DT_CRIACAO FROM AS_PRODUTOS
           WHERE ID_SISTEMA = :idSistema AND CODPROD = :codProd`,
          { idSistema, codProd: produto.CODPROD },
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
    console.log(`📦 [Sync] Processado lote ${Math.floor(i / BATCH_SIZE) + 1} de ${Math.ceil(produtos.length / BATCH_SIZE)}`);
  }

  console.log(`✅ [Sync] UPSERT concluído - Inseridos: ${inseridos}, Atualizados: ${atualizados}`);
  return { inseridos, atualizados };
}

/**
 * Sincroniza produtos de uma empresa específica
 */
export async function sincronizarProdutosPorEmpresa(idSistema: number, empresaNome: string): Promise<SyncResult> {
  const dataInicio = new Date();
  let connection: oracledb.Connection | undefined;

  try {
    console.log(`\n🚀🚀🚀 ================================================`);
    console.log(`🚀 SINCRONIZAÇÃO DE PRODUTOS`);
    console.log(`🚀 ID_SISTEMA: ${idSistema}`);
    console.log(`🚀 Empresa: ${empresaNome}`);
    console.log(`🚀 ================================================\n`);

    // SEMPRE forçar renovação do token para garantir credenciais corretas
    console.log(`🔄 [Sync] Forçando renovação do token para contrato ${idSistema}...`);
    const bearerToken = await obterToken(idSistema, true);
    const produtos = await buscarProdutosSankhya(idSistema, bearerToken);
    connection = await getOracleConnection();

    const registrosDeletados = await marcarTodosComoNaoAtuais(connection, idSistema);
    const { inseridos, atualizados } = await upsertProdutos(connection, idSistema, produtos, bearerToken);

    await connection.commit();

    const dataFim = new Date();
    const duracao = dataFim.getTime() - dataInicio.getTime();

    console.log(`✅ [Sync] Sincronização concluída com sucesso para ${empresaNome}`);
    console.log(`📊 [Sync] Resumo: ${produtos.length} registros, ${inseridos} inseridos, ${atualizados} atualizados, ${registrosDeletados} deletados`);
    console.log(`⏱️ [Sync] Duração: ${duracao}ms`);

    // Salvar log de sucesso
    try {
      const { salvarLogSincronizacao } = await import('./sync-logs-service');
      await salvarLogSincronizacao({
        ID_SISTEMA: idSistema,
        EMPRESA: empresaNome,
        TABELA: 'AS_PRODUTOS',
        STATUS: 'SUCESSO',
        TOTAL_REGISTROS: produtos.length,
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
      totalRegistros: produtos.length,
      registrosInseridos: inseridos,
      registrosAtualizados: atualizados,
      registrosDeletados,
      dataInicio,
      dataFim,
      duracao
    };

  } catch (error: any) {
    console.error(`❌ [Sync] Erro ao sincronizar produtos para ${empresaNome}:`, error);

    if (connection) {
      try {
        await connection.rollback();
      } catch (rollbackError) {
        console.error('❌ [Sync] Erro ao fazer rollback:', rollbackError);
      }
    }

    const dataFim = new Date();
    const duracao = dataFim.getTime() - dataInicio.getTime();

    // Salvar log de falha
    try {
      const { salvarLogSincronizacao } = await import('./sync-logs-service');
      await salvarLogSincronizacao({
        ID_SISTEMA: idSistema,
        EMPRESA: empresaNome,
        TABELA: 'AS_PRODUTOS',
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
    } catch (logError) {
      console.error('❌ [Sync] Erro ao salvar log:', logError);
    }

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
      try {
        await connection.close();
      } catch (closeError) {
        console.error('❌ [Sync] Erro ao fechar conexão:', closeError);
      }
    }
  }
}

/**
 * Sincroniza produtos de todas as empresas ativas (uma por vez)
 */
export async function sincronizarTodasEmpresas(): Promise<SyncResult[]> {
  console.log('🌐 [Sync] Iniciando sincronização de produtos de todas as empresas...');

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

    if (!result.rows || result.rows.length === 0) {
      console.log('⚠️ [Sync] Nenhuma empresa ativa encontrada');
      return [];
    }

    const empresas = result.rows as any[];
    console.log(`📋 [Sync] ${empresas.length} empresas ativas encontradas`);

    // Sincronizar sequencialmente (uma por vez)
    for (const empresa of empresas) {
      const resultado = await sincronizarProdutosPorEmpresa(
        empresa.ID_EMPRESA,
        empresa.EMPRESA
      );
      resultados.push(resultado);

      // Aguardar 2 segundos entre sincronizações
      await new Promise(resolve => setTimeout(resolve, 2000));
    }

    const sucessos = resultados.filter(r => r.success).length;
    const falhas = resultados.filter(r => !r.success).length;

    console.log(`🏁 [Sync] Sincronização de todas as empresas concluída`);
    console.log(`✅ Sucessos: ${sucessos}, ❌ Falhas: ${falhas}`);

    return resultados;

  } catch (error: any) {
    console.error('❌ [Sync] Erro ao sincronizar todas as empresas:', error);
    throw error;
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (closeError) {
        console.error('❌ [Sync] Erro ao fechar conexão:', closeError);
      }
    }
  }
}

/**
 * Obter estatísticas de sincronização
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
       FROM AS_PRODUTOS
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
 * Listar produtos sincronizados
 */
export async function listarProdutos(idSistema?: number) {
  const connection = await getOracleConnection();

  try {
    const whereClause = idSistema ? `WHERE P.ID_SISTEMA = :idSistema` : '';
    const params = idSistema ? { idSistema } : {};

    const result = await connection.execute(
      `SELECT 
                P.ID_SISTEMA,
                C.EMPRESA as NOME_CONTRATO,
                P.CODPROD,
                P.DESCRPROD,
                P.SANKHYA_ATUAL,
                P.DT_ULT_CARGA
            FROM AS_PRODUTOS P
            JOIN AD_CONTRATOS C ON C.ID_EMPRESA = P.ID_SISTEMA
            ${whereClause}
            ORDER BY P.ID_SISTEMA, P.DESCRPROD
            FETCH FIRST 500 ROWS ONLY`,
      params,
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );

    return result.rows || [];
  } finally {
    await connection.close();
  }
}