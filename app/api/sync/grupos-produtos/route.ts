
import { NextRequest, NextResponse } from 'next/server';
import {
    sincronizarGruposProdutosPorEmpresa,
    sincronizarTodasEmpresas,
    obterEstatisticasSincronizacao,
    listarGruposProdutos
} from '@/lib/sync-grupos-produtos-service';

export async function GET(request: NextRequest) {
    try {
        const searchParams = request.nextUrl.searchParams;
        const idSistema = searchParams.get('idSistema');
        const list = searchParams.get('list');

        if (list === 'true') {
            const data = await listarGruposProdutos(idSistema ? Number(idSistema) : undefined);
            return NextResponse.json(data);
        }

        if (idSistema) {
            const stats = await obterEstatisticasSincronizacao(Number(idSistema));
            return NextResponse.json(stats);
        }

        const stats = await obterEstatisticasSincronizacao();
        return NextResponse.json(stats);
    } catch (error: any) {
        return NextResponse.json(
            { error: error.message || 'Erro ao buscar estatísticas de grupos de produtos' },
            { status: 500 }
        );
    }
}

export async function POST(request: NextRequest) {
    try {
        const body = await request.json();
        const { idSistema, empresa, syncAll } = body;

        if (syncAll) {
            // Sincronizar todos
            sincronizarTodasEmpresas().catch(console.error);

            return NextResponse.json({
                message: 'Sincronização de todos os grupos de produtos iniciada em background'
            });
        }

        if (!idSistema || !empresa) {
            return NextResponse.json(
                { error: 'idSistema e empresa são obrigatórios para sincronização individual' },
                { status: 400 }
            );
        }

        // Sincronizar apenas uma empresa
        const resultado = await sincronizarGruposProdutosPorEmpresa(idSistema, empresa);

        return NextResponse.json(resultado);

    } catch (error: any) {
        return NextResponse.json(
            { error: error.message || 'Erro ao sincronizar grupos de produtos' },
            { status: 500 }
        );
    }
}
