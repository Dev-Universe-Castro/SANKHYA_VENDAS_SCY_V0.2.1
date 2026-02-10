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

/**
 * Faz requisição autenticada usando um Bearer Token específico
 * Usado durante sincronização para garantir que usamos o Bearer Token correto
 */
async function fazerRequisicaoAutenticadaComBearer(url: string, bearerToken: string, data: any, retryCount: number = 0): Promise<any> {
  const axios = require('axios');
  const MAX_RETRIES = 3;

  const config = {
    method: 'POST',
    url: url,
    data: data,
    headers: {
      'Authorization': `Bearer ${bearerToken}`,
      'Content-Type': 'application/json'
    },
    timeout: 60000
  };

  try {
    const response = await axios(config);
    return response.data;
  } catch (error: any) {
    // Se erro de autenticação (401/403), propagar erro especial para renovar token
    if (error.response?.status === 401 || error.response?.status === 403) {
      console.log('🔑 [Sync] Token expirado detectado (401/403) - propagando para renovação...');
      const authError: any = new Error('TOKEN_EXPIRED');
      authError.isAuthError = true;
      authError.originalError = error;
      throw authError;
    }

    // Retry apenas em erros de rede/timeout
    if (retryCount < MAX_RETRIES &&
      (error.code === 'ECONNABORTED' || error.code === 'ETIMEDOUT' || error.response?.status >= 500)) {
      console.log(`🔄 [Sync] Retry ${retryCount + 1}/${MAX_RETRIES} após erro de rede...`);
      await new Promise(resolve => setTimeout(resolve, 2000 * (retryCount + 1)));
      return fazerRequisicaoAutenticadaComBearer(url, bearerToken, data, retryCount + 1);
    }

    console.error('❌ [Sync] Erro na requisição:', error.response?.data || error.message);
    throw new Error(`Erro ao buscar dados: ${error.response?.data?.statusMessage || error.message}`);
  }
}

interface ParceiroSankhya {
  CODPARC: number;
  NOMEPARC: string;
  CGC_CPF?: string;
  CODCID?: number;
  ATIVO?: string;
  TIPPESSOA?: string;
  RAZAOSOCIAL?: string;
  IDENTINSCESTAD?: string;
  CEP?: string;
  CODEND?: number;
  NUMEND?: string;
  COMPLEMENTO?: string;
  CODBAI?: number;
  LATITUDE?: string;
  LONGITUDE?: string;
  CLIENTE?: string;
  CODVEND?: number;
  CODREG?: number;
  CODTAB?: number;
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
 * Busca todos os parceiros do Sankhya para uma empresa específica
 * @param idSistema - ID do sistema/contrato
 * @param bearerToken - Bearer Token específico da empresa (será renovado automaticamente se expirar)
 */
async function buscarParceirosSankhya(idSistema: number, bearerToken: string): Promise<ParceiroSankhya[]> {
  console.log(`🔍 [Sync] Buscando parceiros usando Bearer Token: ${bearerToken.substring(0, 50)}...`);

  let allParceiros: ParceiroSankhya[] = [];
  let currentPage = 0;
  let hasMoreData = true;
  let currentToken = bearerToken;

  while (hasMoreData) {
    console.log(`📄 [Sync] Buscando página ${currentPage} de parceiros...`);

    const PAYLOAD = {
      "requestBody": {
        "dataSet": {
          "rootEntity": "Parceiro",
          "includePresentationFields": "N",
          "useFileBasedPagination": true,
          "disableRowsLimit": true,
          "offsetPage": currentPage.toString(),
          "entity": {
            "fieldset": {
              "list": "CODPARC, NOMEPARC, CGC_CPF, CODCID, ATIVO, TIPPESSOA, RAZAOSOCIAL, IDENTINSCESTAD, CEP, CODEND, NUMEND, COMPLEMENTO, CODBAI, LATITUDE, LONGITUDE, CLIENTE, CODVEND, CODREG, CODTAB"
            }
          }
        }
      }
    };

    try {
      const respostaCompleta = await fazerRequisicaoAutenticadaComBearer(
        URL_CONSULTA_SERVICO,
        currentToken,
        PAYLOAD
      );

      const entities = respostaCompleta.responseBody?.entities;

      if (!entities || !entities.entity) {
        console.log(`⚠️ [Sync] Nenhum parceiro encontrado na página ${currentPage}`);
        break;
      }

      const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
      const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];

      const parceirosPagina = entityArray.map((rawEntity: any) => {
        const cleanObject: any = {};
        for (let i = 0; i < fieldNames.length; i++) {
          const fieldKey = `f${i}`;
          const fieldName = fieldNames[i];
          if (rawEntity[fieldKey]) {
            cleanObject[fieldName] = rawEntity[fieldKey].$;
          }
        }
        return cleanObject as ParceiroSankhya;
      });

      allParceiros = allParceiros.concat(parceirosPagina);
      console.log(`✅ [Sync] Página ${currentPage}: ${parceirosPagina.length} registros (total acumulado: ${allParceiros.length})`);

      const hasMoreResult = entities.hasMoreResult === 'true' || entities.hasMoreResult === true;

      if (parceirosPagina.length === 0 || !hasMoreResult) {
        hasMoreData = false;
        console.log(`🏁 [Sync] Última página atingida (hasMoreResult: ${hasMoreResult}, registros: ${parceirosPagina.length})`);
      } else {
        currentPage++;
        await new Promise(resolve => setTimeout(resolve, 500));
      }
    } catch (pageError: any) {
      // Se token expirou (detectado pelo erro especial), renovar e retentar
      if (pageError.isAuthError) {
        console.log(`🔄 [Sync] Token expirado na página ${currentPage}, renovando...`);
        console.log(`📊 [Sync] Progresso mantido: ${allParceiros.length} registros acumulados`);
        try {
          currentToken = await obterToken(idSistema, true);
          console.log(`✅ [Sync] Novo token obtido: ${currentToken.substring(0, 50)}...`);
          console.log(`🔁 [Sync] Retentando página ${currentPage} com novo token...`);
          // NÃO incrementar currentPage - vai retentar a mesma página
          await new Promise(resolve => setTimeout(resolve, 1000));
        } catch (tokenError) {
          console.error(`❌ [Sync] Erro ao renovar token:`, tokenError);
          throw new Error(`Falha ao renovar token após expiração: ${tokenError}`);
        }
      } else {
        // Outros erros são fatais
        console.error(`❌ [Sync] Erro fatal na página ${currentPage}:`, pageError.message);
        throw pageError;
      }
    }
  }

  console.log(`✅ [Sync] Total de ${allParceiros.length} parceiros recuperados em ${currentPage} páginas`);
  return allParceiros;
}

/**
 * Executa o soft delete (marca como não atual) todos os parceiros do sistema
 */
async function marcarTodosComoNaoAtuais(connection: oracledb.Connection, idSistema: number): Promise<number> {
  const result = await connection.execute(
    `UPDATE AS_PARCEIROS 
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
 * Executa UPSERT de parceiros usando MERGE
 */
async function upsertParceiros(
  connection: oracledb.Connection,
  idSistema: number,
  parceiros: ParceiroSankhya[]
): Promise<{ inseridos: number; atualizados: number }> {
  let inseridos = 0;
  let atualizados = 0;

  // Processar em lotes de 100 para evitar sobrecarga
  const BATCH_SIZE = 100;

  for (let i = 0; i < parceiros.length; i += BATCH_SIZE) {
    const batch = parceiros.slice(i, i + BATCH_SIZE);

    for (const parceiro of batch) {
      try {
        // Truncar latitude/longitude se necessário (máximo 50 caracteres)
        const latitude = parceiro.LATITUDE ? String(parceiro.LATITUDE).substring(0, 50) : null;
        const longitude = parceiro.LONGITUDE ? String(parceiro.LONGITUDE).substring(0, 50) : null;

        const result = await connection.execute(
          `MERGE INTO AS_PARCEIROS dest
         USING (SELECT :idSistema AS ID_SISTEMA, :codParc AS CODPARC FROM DUAL) src
         ON (dest.ID_SISTEMA = src.ID_SISTEMA AND dest.CODPARC = src.CODPARC)
         WHEN MATCHED THEN
           UPDATE SET
             NOMEPARC = :nomeparc,
             CGC_CPF = :cgfCpf,
             CODCID = :codCid,
             ATIVO = :ativo,
             TIPPESSOA = :tipPessoa,
             RAZAOSOCIAL = :razaoSocial,
             IDENTINSCESTAD = :identInscEstad,
             CEP = :cep,
             CODEND = :codEnd,
             NUMEND = :numEnd,
             COMPLEMENTO = :complemento,
             CODBAI = :codBai,
             LATITUDE = :latitude,
             LONGITUDE = :longitude,
              CLIENTE = :cliente,
              CODVEND = :codVend,
              CODREG = :codReg,
              CODTAB = :codTab,
              SANKHYA_ATUAL = 'S',
             DT_ULT_CARGA = CURRENT_TIMESTAMP
         WHEN NOT MATCHED THEN
           INSERT (
             ID_SISTEMA, CODPARC, NOMEPARC, CGC_CPF, CODCID, ATIVO, TIPPESSOA,
              RAZAOSOCIAL, IDENTINSCESTAD, CEP, CODEND, NUMEND, COMPLEMENTO,
              CODBAI, LATITUDE, LONGITUDE, CLIENTE, CODVEND, CODREG, CODTAB, SANKHYA_ATUAL,
              DT_ULT_CARGA, DT_CRIACAO
           )
           VALUES (
             :idSistema, :codParc, :nomeparc, :cgfCpf, :codCid, :ativo, :tipPessoa,
              :razaoSocial, :identInscEstad, :cep, :codEnd, :numEnd, :complemento,
              :codBai, :latitude, :longitude, :cliente, :codVend, :codReg, :codTab, 'S',
              CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
           )`,
          {
            idSistema,
            codParc: parceiro.CODPARC,
            nomeparc: parceiro.NOMEPARC || null,
            cgfCpf: parceiro.CGC_CPF || null,
            codCid: parceiro.CODCID || null,
            ativo: parceiro.ATIVO || null,
            tipPessoa: parceiro.TIPPESSOA || null,
            razaoSocial: parceiro.RAZAOSOCIAL || null,
            identInscEstad: parceiro.IDENTINSCESTAD || null,
            cep: parceiro.CEP || null,
            codEnd: parceiro.CODEND || null,
            numEnd: parceiro.NUMEND || null,
            complemento: parceiro.COMPLEMENTO || null,
            codBai: parceiro.CODBAI || null,
            latitude: latitude,
            longitude: longitude,
            cliente: parceiro.CLIENTE || null,
            codVend: parceiro.CODVEND || null,
            codReg: parceiro.CODREG || null,
            codTab: parceiro.CODTAB || null
          },
          { autoCommit: false }
        );

        // Oracle não retorna se foi INSERT ou UPDATE no MERGE, então estimamos
        // Se rowsAffected > 0, foi uma operação bem-sucedida
        if (result.rowsAffected && result.rowsAffected > 0) {
          // Verificar se era novo ou atualização
          const checkResult = await connection.execute(
            `SELECT DT_CRIACAO FROM AS_PARCEIROS 
           WHERE ID_SISTEMA = :idSistema AND CODPARC = :codParc`,
            { idSistema, codParc: parceiro.CODPARC },
            { outFormat: oracledb.OUT_FORMAT_OBJECT }
          );

          if (checkResult.rows && checkResult.rows.length > 0) {
            const row: any = checkResult.rows[0];
            const dtCriacao = new Date(row.DT_CRIACAO);
            const agora = new Date();
            const diferencaMs = agora.getTime() - dtCriacao.getTime();

            // Se foi criado há menos de 5 segundos, consideramos como inserção
            if (diferencaMs < 5000) {
              inseridos++;
            } else {
              atualizados++;
            }
          }
        }
      } catch (error: any) {
        console.error(`❌ [Sync] Erro ao processar parceiro ${parceiro.CODPARC}:`, error.message);
        // Continua processando os próximos mesmo com erro individual
      }
    }

    // Commit apenas ao final do lote (não a cada registro)
    console.log(`📦 [Sync] Lote ${Math.floor(i / BATCH_SIZE) + 1} de ${Math.ceil(parceiros.length / BATCH_SIZE)} processado, fazendo commit...`);
    await connection.commit();
  }

  console.log(`✅ [Sync] UPSERT concluído - Inseridos: ${inseridos}, Atualizados: ${atualizados}`);
  return { inseridos, atualizados };
}

/**
 * Sincroniza parceiros de uma empresa específica
 */
export async function sincronizarParceirosPorEmpresa(idSistema: number, empresaNome: string): Promise<SyncResult> {
  const dataInicio = new Date();
  let connection: oracledb.Connection | undefined;

  try {
    console.log(`\n🚀🚀🚀 ================================================`);
    console.log(`🚀 SINCRONIZAÇÃO DE PARCEIROS`);
    console.log(`🚀 ID_SISTEMA: ${idSistema}`);
    console.log(`🚀 Empresa: ${empresaNome}`);
    console.log(`🚀 ================================================\n`);

    // CRÍTICO: Gerar novo Bearer Token SEMPRE antes de sincronizar
    // Cada empresa tem credenciais únicas que geram Bearer Tokens diferentes
    // NÃO usar cache durante sincronização para evitar uso de Bearer Token errado
    console.log(`🔄 [Sync] Gerando novo Bearer Token para contrato ${idSistema}...`);
    const bearerToken = await obterToken(idSistema, true);
    console.log(`✅ [Sync] Bearer Token obtido: ${bearerToken.substring(0, 50)}...`);

    // Buscar parceiros do Sankhya usando o Bearer Token específico
    const parceiros = await buscarParceirosSankhya(idSistema, bearerToken);

    // Conectar ao Oracle
    connection = await getOracleConnection();

    // Fase 1: Soft Delete (marcar todos como não atuais)
    const registrosDeletados = await marcarTodosComoNaoAtuais(connection, idSistema);

    // Fase 2: UPSERT (inserir/atualizar)
    const { inseridos, atualizados } = await upsertParceiros(connection, idSistema, parceiros);

    // Commit final
    await connection.commit();

    const dataFim = new Date();
    const duracao = dataFim.getTime() - dataInicio.getTime();

    console.log(`✅ [Sync] Sincronização concluída com sucesso para ${empresaNome}`);
    console.log(`📊 [Sync] Resumo: ${parceiros.length} registros, ${inseridos} inseridos, ${atualizados} atualizados, ${registrosDeletados} deletados`);
    console.log(`⏱️ [Sync] Duração: ${duracao}ms`);

    // Salvar log de sucesso
    const { salvarLogSincronizacao } = await import('./sync-logs-service');
    await salvarLogSincronizacao({
      ID_SISTEMA: idSistema,
      EMPRESA: empresaNome,
      TABELA: 'AS_PARCEIROS',
      STATUS: 'SUCESSO',
      TOTAL_REGISTROS: parceiros.length,
      REGISTROS_INSERIDOS: inseridos,
      REGISTROS_ATUALIZADOS: atualizados,
      REGISTROS_DELETADOS: registrosDeletados,
      DURACAO_MS: duracao,
      DATA_INICIO: dataInicio,
      DATA_FIM: dataFim
    });

    return {
      success: true,
      idSistema,
      empresa: empresaNome,
      totalRegistros: parceiros.length,
      registrosInseridos: inseridos,
      registrosAtualizados: atualizados,
      registrosDeletados,
      dataInicio,
      dataFim,
      duracao
    };

  } catch (error: any) {
    console.error(`❌ [Sync] Erro ao sincronizar parceiros para ${empresaNome}:`, error);

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
        TABELA: 'AS_PARCEIROS',
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
 * Sincroniza parceiros de todas as empresas ativas
 */
export async function sincronizarTodasEmpresas(): Promise<SyncResult[]> {
  console.log('🌐 [Sync] Iniciando sincronização de todas as empresas...');

  let connection: oracledb.Connection | undefined;
  const resultados: SyncResult[] = [];

  try {
    connection = await getOracleConnection();

    // Buscar todas as empresas ativas
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

    // Sincronizar cada empresa sequencialmente (uma por vez)
    for (const empresa of empresas) {
      console.log(`🔄 [Sync] Sincronizando empresa ${empresa.EMPRESA} (${empresa.ID_EMPRESA})...`);

      const resultado = await sincronizarParceirosPorEmpresa(
        empresa.ID_EMPRESA,
        empresa.EMPRESA
      );
      resultados.push(resultado);

      console.log(`✓ [Sync] Empresa ${empresa.EMPRESA} concluída`);

      // Aguardar 3 segundos entre sincronizações para evitar sobrecarga
      await new Promise(resolve => setTimeout(resolve, 3000));
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
       FROM AS_PARCEIROS
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
 * Listar parceiros sincronizados (Para o frontend)
 */
export async function listarParceiros(idSistema?: number) {
  const connection = await getOracleConnection();

  try {
    const whereClause = idSistema ? `WHERE P.ID_SISTEMA = :idSistema` : '';
    const params = idSistema ? { idSistema } : {};

    const result = await connection.execute(
      `SELECT 
                P.ID_SISTEMA,
                AC.EMPRESA as NOME_CONTRATO,
                P.CODPARC,
                P.NOMEPARC,
                P.SANKHYA_ATUAL,
                P.DT_ULT_CARGA
            FROM AS_PARCEIROS P
            JOIN AD_CONTRATOS AC ON AC.ID_EMPRESA = P.ID_SISTEMA
            ${whereClause}
            ORDER BY P.ID_SISTEMA, P.NOMEPARC
            FETCH FIRST 500 ROWS ONLY`, // Limitando para não sobrecarregar
      params,
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );

    return result.rows || [];
  } finally {
    await connection.close();
  }
}