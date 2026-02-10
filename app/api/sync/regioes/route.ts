
import { NextRequest, NextResponse } from 'next/server';
import {
    sincronizarRegioesPorEmpresa,
    sincronizarTodasRegioes,
    obterEstatisticasSincronizacao,
    listarRegioes
} from '@/lib/sync-regioes-service';

export async function GET(request: NextRequest) {
    try {
        const searchParams = request.nextUrl.searchParams;
        const idSistema = searchParams.get('idSistema');
        const list = searchParams.get('list');

        if (list === 'true') {
            const data = await listarRegioes(idSistema ? Number(idSistema) : undefined);
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
            { error: error.message || 'Erro ao buscar estatísticas de regiões' },
            { status: 500 }
        );
    }
}

export async function POST(request: NextRequest) {
    try {
        const body = await request.json();
        const { idSistema, empresa, syncAll } = body;

        if (syncAll) {
            // Sincronizar todas as regiões
            sincronizarTodasRegioes().catch(console.error);

            return NextResponse.json({
                message: 'Sincronização de todas as regiões iniciada em background'
            });
        }

        if (!idSistema || !empresa) {
            return NextResponse.json(
                { error: 'idSistema e empresa são obrigatórios para sincronização individual' },
                { status: 400 }
            );
        }

        // Sincronizar apenas uma empresa
        const resultado = await sincronizarRegioesPorEmpresa(idSistema, empresa);

        return NextResponse.json(resultado);

    } catch (error: any) {
        return NextResponse.json(
            { error: error.message || 'Erro ao sincronizar regiões' },
            { status: 500 }
        );
    }
}
