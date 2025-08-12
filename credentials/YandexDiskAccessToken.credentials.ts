import type { ICredentialType, INodeProperties } from 'n8n-workflow';

export class YandexDiskAccessToken implements ICredentialType {
	name = 'yandexDiskAccessToken';
	displayName = 'Yandex Disk Access Token';
	documentationUrl = 'https://yandex.ru/dev/disk/rest/';
	properties: INodeProperties[] = [
		{
			displayName: 'Access Token',
			name: 'accessToken',
			type: 'string',
			default: '',
			typeOptions: { password: true },
			description:
				'Введите OAuth-токен Яндекс.Диска. Формат заголовка будет `Authorization: OAuth <token>`',
		},
	];
}