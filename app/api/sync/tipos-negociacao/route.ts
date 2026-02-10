
import { NextRequest, NextResponse } from 'next/server';
import {
  sincronizarTiposNegociacaoPorEmpresa,
  sincronizarTodasEmpresas,
  obterEstatisticasSincronizacao,
  listarTiposNegociacao
} from '@/lib/sync-tipos-negociacao-service';

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const idSistema = searchParams.get('idSistema');
    const list = searchParams.get('list');

    if (list === 'true') {
      const data = await listarTiposNegociacao(idSistema ? Number(idSistema) : undefined);
      return NextResponse.json(data);
    }

    if (idSistema) {
      const stats = await obterEstatisticasSincronizacao(parseInt(idSistema));
      return NextResponse.json(stats);
    }

    const stats = await obterEstatisticasSincronizacao();
    return NextResponse.json(stats);
  } catch (error: any) {
    console.error('Erro ao obter estatísticas:', error);
    return NextResponse.json(
      { error: error.message || 'Erro ao obter estatísticas' },
      { status: 500 }
    );
  }
}

export async function POST(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const idSistema = searchParams.get('idSistema');
    const empresa = searchParams.get('empresa');

    console.log('📥 [API] Requisição de sincronização recebida:', { idSistema, empresa });

    if (idSistema && empresa) {
      console.log(`🔄 [API] Sincronizando empresa: ${empresa} (ID: ${idSistema})`);
      const resultado = await sincronizarTiposNegociacaoPorEmpresa(
        parseInt(idSistema),
        empresa
      );
      return NextResponse.json(resultado);
    }

    console.log('🔄 [API] Sincronizando todas as empresas');
    const resultados = await sincronizarTodasEmpresas();
    return NextResponse.json(resultados);
  } catch (error: any) {
    console.error('❌ [API] Erro ao sincronizar tipos de negociação:', error);
    return NextResponse.json(
      { error: error.message || 'Erro ao sincronizar tipos de negociação' },
      { status: 500 }
    );
  }
}
