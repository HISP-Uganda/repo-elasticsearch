"use strict";

/**
 * @typedef {import('moleculer').Context} Context Moleculer's Context
 */

module.exports = {
	name: "research",
	/**
	 * Settings
	 */
	settings: {},

	/**
	 * Dependencies
	 */
	dependencies: ["es"],

	/**
	 * Actions
	 */
	actions: {
		search: {
			rest: {
				method: "POST",
				path: "/search",
			},
			async handler(ctx) {
				const { index, ...body } = ctx.params;
				return await ctx.call("es.search", {
					index,
					body,
				});
			},
		},
		aggregate: {
			rest: {
				method: "POST",
				path: "/",
			},
			async handler(ctx) {
				const { index, ...body } = ctx.params;
				return await ctx.call("es.aggregations", {
					index,
					body,
				});
			},
		},
		index: {
			rest: {
				method: "POST",
				path: "/index",
			},
			async handler(ctx) {
				const { index, data, pk } = ctx.params;
				try {
					if (pk) {
						return await ctx.call("es.bulk", {
							index,
							dataset: data,
							id: pk,
						});
					}
				} catch (error) {
					console.log(error);
					return error;
				}
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
