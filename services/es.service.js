"use strict";
const { Client } = require("@elastic/elasticsearch");
const { flatMap } = require("lodash");

// const client = new Client({ node: "http://localhost:9200" });
const client = new Client({ node: "http://192.168.64.3:9200" });

/**
 * @typedef {import('moleculer').Context} Context Moleculer's Context
 */

module.exports = {
	name: "es",
	/**
	 * Settings
	 */
	settings: {},

	/**
	 * Dependencies
	 */
	dependencies: [],

	/**
	 * Actions
	 */
	actions: {
		createIndex: {
			params: {
				index: "string",
				body: "object|optional",
			},
			async handler(ctx) {
				let body = {
					index: ctx.params.index,
				};
				if (ctx.params.body) {
					body = { ...body, body: ctx.params.body };
				}
				return await client.indices.create(body);
			},
		},
		bulk: {
			async handler(ctx) {
				const { index, dataset, id } = ctx.params;
				const body = flatMap(dataset, (doc) => [
					{ index: { _index: index, _id: doc[id] } },
					doc,
				]);
				const { body: bulkResponse } = await client.bulk({
					refresh: true,
					body,
				});
				const errorDocuments = [];
				if (bulkResponse.errors) {
					bulkResponse.items.forEach((action, i) => {
						const operation = Object.keys(action)[0];
						if (action[operation].error) {
							errorDocuments.push({
								status: action[operation].status,
								error: action[operation].error,
								operation: body[i * 2],
								document: body[i * 2 + 1],
							});
						}
					});
				}
				return {
					errorDocuments,
					inserted: dataset.length - errorDocuments.length,
				};
			},
		},
		search: {
			params: {
				index: "string",
				body: "object",
			},
			async handler(ctx) {
				const {
					body: {
						hits: { hits },
					},
				} = await client.search({
					index: ctx.params.index,
					body: ctx.params.body,
				});
				return hits;
			},
		},
		aggregations: {
			params: {
				index: "string",
				body: "object",
			},
			async handler(ctx) {
				const {
					body: { aggregations },
				} = await client.search({
					index: ctx.params.index,
					body: ctx.params.body,
				});
				return aggregations;
			},
		},
	},

	/**
	 * Events
	 */
	events: {},

	/**
	 * Methods
	 */
	methods: {},

	/**
	 * Service created lifecycle event handler
	 */
	created() {},

	/**
	 * Service started lifecycle event handler
	 */
	async started() {},

	/**
	 * Service stopped lifecycle event handler
	 */
	async stopped() {},
};
