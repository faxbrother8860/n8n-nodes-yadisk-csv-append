import type {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	IDataObject,
} from 'n8n-workflow';

import { stringify } from 'csv-stringify/sync';
import { parse } from 'csv-parse/sync';

function ensureTrailingNewline(s: string) {
	return s.endsWith('\n') ? s : s + '\n';
}

export class YandexDiskCsvAppend implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Yandex Disk CSV: Append Row',
		name: 'yandexDiskCsvAppend',
		icon: 'file:icons/yandex.svg',
		group: ['transform'],
		version: 1,
		description:
			'Добавляет одну или несколько строк в CSV на Яндекс.Диске (аналог Append Row для Google Sheets)',
		defaults: { name: 'Yandex Disk CSV: Append Row' },
		inputs: ['main'],
		outputs: ['main'],
		credentials: [
			{ name: 'yandexDiskAccessToken', required: true },
		],
		properties: [
			{
				displayName: 'File Path',
				name: 'filePath',
				type: 'string',
				default: 'disk:/path/to/file.csv',
				description: 'Путь к файлу на Яндекс.Диске, например: disk:/reports/data.csv',
			},
			{
				displayName: 'Delimiter',
				name: 'delimiter',
				type: 'options',
				options: [
					{ name: 'Comma (,)', value: ',' },
					{ name: 'Semicolon (;)', value: ';' },
					{ name: 'Tab (\t)', value: '\t' },
				],
				default: ',',
			},
			{
				displayName: 'Encoding',
				name: 'encoding',
				type: 'options',
				options: [
					{ name: 'UTF-8', value: 'utf-8' },
					{ name: 'Windows-1251', value: 'windows-1251' },
				],
				default: 'utf-8',
				description: 'Для кириллицы обычно подходит UTF-8. CP1251 поддерживается на свой страх и риск.',
			},
			{
				displayName: 'Has Header',
				name: 'hasHeader',
				type: 'boolean',
				default: true,
				description: 'Первая строка CSV содержит названия колонок',
			},
			{
				displayName: 'Mapping Mode',
				name: 'mappingMode',
				type: 'options',
				options: [
					{ name: 'Auto-map by Header', value: 'byHeader' },
					{ name: 'By Explicit Columns', value: 'byColumns' },
				],
				default: 'byHeader',
			},
			{
				displayName: 'Columns (order)',
				name: 'columns',
				type: 'string',
				default: '',
				description: 'Список имён колонок через запятую, если выбран режим By Explicit Columns',
				displayOptions: {
					show: { mappingMode: ['byColumns'] },
				},
			},
			{
				displayName: 'Create If Missing',
				name: 'createIfMissing',
				type: 'boolean',
				default: true,
			},
			{
				displayName: 'Write Header If Creating',
				name: 'writeHeaderOnCreate',
				type: 'boolean',
				default: true,
				displayOptions: { show: { createIfMissing: [true] } },
			},
		],
	};

	async execute(this: IExecuteFunctions) {
		const items = this.getInputData();
		const filePath = this.getNodeParameter('filePath', 0) as string;
		const delimiter = this.getNodeParameter('delimiter', 0) as string;
		const encoding = (this.getNodeParameter('encoding', 0) as string) || 'utf-8';
		const hasHeader = this.getNodeParameter('hasHeader', 0) as boolean;
		const mappingMode = this.getNodeParameter('mappingMode', 0) as 'byHeader' | 'byColumns';
		const createIfMissing = this.getNodeParameter('createIfMissing', 0) as boolean;
		const writeHeaderOnCreate = this.getNodeParameter('writeHeaderOnCreate', 0) as boolean;
		const columnsCsv = this.getNodeParameter('columns', 0, '') as string;
		const explicitColumns = columnsCsv
			.split(',')
			.map((s) => s.trim())
			.filter((s) => s.length > 0);

		const creds = await this.getCredentials('yandexDiskAccessToken');
		const token = (creds as IDataObject).accessToken as string;

		const base = 'https://cloud-api.yandex.net/v1/disk';
		const headersAuth = { Authorization: `OAuth ${token}`, 'User-Agent': 'n8n-yadisk-csv/0.1.0' };

		// 1) Получим ссылку на скачивание
		const dlLinkResp = await this.helpers.httpRequest({
			url: `${base}/resources/download`,
			method: 'GET',
			qs: { path: filePath },
			headers: headersAuth,
			json: true,
			returnFullResponse: true,
		}).catch(async (err) => {
			// Если файла нет и разрешено создавать — пропустим скачивание
			if (err?.response?.statusCode === 404 && createIfMissing) return null;
			throw err;
		});

		let currentCsv = '';
		let headerRow: string[] | null = null;

		if (dlLinkResp?.body?.href) {
			const downloadUrl = dlLinkResp.body.href as string;
			const fileResp = await this.helpers.httpRequest({ url: downloadUrl, method: 'GET', encoding: 'text' });
			currentCsv = (fileResp as string) || '';
			if (hasHeader && currentCsv.trim().length > 0) {
				const parsed = parse(currentCsv.split(/\r?\n/)[0], { delimiter, relax_column_count: true }) as string[][];
				headerRow = parsed[0] ?? null;
			}
		}

		// 2) Построим новые строки
		const rows: string[][] = [];
		for (let i = 0; i < items.length; i++) {
			const obj = items[i].json as IDataObject;
			let values: (string | number | null)[] = [];
			if (mappingMode === 'byHeader') {
				if (!headerRow) {
					// Нет файла или пусто: возьмём ключи из первого item
					headerRow = Object.keys(obj);
				}
				values = headerRow.map((key) => (obj[key] as any) ?? '');
			} else {
				values = explicitColumns.map((key) => (obj[key] as any) ?? '');
			}
			rows.push(values.map((v) => (v === null || v === undefined ? '' : String(v))));
		}

		// 3) Сформируем CSV фрагмент
		let appendCsv = stringify(rows, { delimiter, record_delimiter: '\n' });

		// 4) Если создаём файл с нуля — допишем header
		if (!dlLinkResp && createIfMissing) {
			if (hasHeader && writeHeaderOnCreate) {
				const header = mappingMode === 'byHeader' ? headerRow ?? [] : explicitColumns;
				const headerCsv = stringify([header], { delimiter, record_delimiter: '\n' });
				appendCsv = headerCsv + appendCsv;
			}
		}

		// 5) Склеим итоговый CSV
		let finalCsv = currentCsv;
		if (finalCsv && appendCsv) finalCsv = ensureTrailingNewline(finalCsv) + appendCsv;
		else if (!finalCsv) finalCsv = appendCsv; // новый файл

		// 6) Загрузим обратно
		const upLinkResp = await this.helpers.httpRequest({
			url: `${base}/resources/upload`,
			method: 'GET',
			qs: { path: filePath, overwrite: 'true' },
			headers: headersAuth,
			json: true,
		});
		const uploadUrl = (upLinkResp as any).href as string;

		await this.helpers.httpRequest({
			url: uploadUrl,
			method: 'PUT',
			body: finalCsv,
			headers: { 'Content-Type': 'text/csv; charset=' + encoding, 'User-Agent': 'n8n-yadisk-csv/0.1.0' },
		});

		const out: INodeExecutionData[] = items.map((it) => ({ json: { ...it.json, _ydisk: { filePath } } }));
		return this.prepareOutputData(out);
	}
}