module.exports = {
    /**
     * <p>
     * This method will connect to es clients and search using given
     * searchQueryJson. In case of invalid searchQueryJson or ES is down it will
     * callback with error same error that ES gives
     * </p>
     *
     * @param {JSON} searchQueryJson query json to search from ES
     */
    search: function search(searchQueryJson, callback) {
        let self = this;
        sails.esClients.search(searchQueryJson, function (error, searchResponseFromES) {
            console.log("EsOperationHelper.search()");
            if (error) {
                console.error("error while searching from ES=" + JSON.stringify(error));
                // callback(error);
                self.createIndexIfMissing(error, searchQueryJson["index"], callback);
            } else {
                callback(null, getSearchResponseJson(searchResponseFromES["body"]));
            }
        });
    },
    /**
     *<p>This is wrapper method will call search API and callback with direct response</p>
     *
     */
    searchWithDirectResponse: function (searchQueryJson, callback) {
        sails.esClients.search(searchQueryJson, function (error, responseFromES) {
            if (error) {
                return callback(error);
            } else {
                responseFromES = responseFromES.body;
                let responseJson = {};
                responseJson["hits"] = responseFromES["hits"];
                responseJson["scroll_id"] = responseFromES["scroll_id"];
                responseJson["aggregations"] = responseFromES["aggregations"];
                callback(null, responseJson);
            }
        });
    },
    /**
     *<p>This is wrapper method will call scroll API and callback with direct response</p>
     *
     */
    scrollWithDirectResponse: function (scrollId, scrollTime, callback) {
        if (!!scrollTime) {
            scrollTime = "20s"; // or default from config.
        }
        sails.esClients.scroll({
            scrollId: scrollId,
            scroll: scrollTime
        }, function (error, responseFromES) {
            if (error) {
                return callback(error);
            } else {
                responseFromES = responseFromES.body;
                let responseJson = {};
                responseJson["hits"] = responseFromES["hits"];
                responseJson["scroll_id"] = responseFromES["_scroll_id"];
                callback(null, responseJson);
            }
        });
    },
    searchAll: function (searchQueryJson, scrollTime, callback) {
        console.log("EsOperationHelper.searchAll()");
        if ("function" === typeof scrollTime) {
            callback = scrollTime;
            scrollTime = "20s";
        }
        if (!!searchQueryJson["scroll"]) {
            searchQueryJson["scroll"] = scrollTime;
        }
        if (!!searchQueryJson["size"]) {
            searchQueryJson["size"] = 10000;
        }
        console.log("searchQueryJson=" + JSON.stringify(searchQueryJson));
        let allData = [];
        sails.esClients.search(searchQueryJson, function getMoreUntilDone(error, response) {
            response = response.body;
            if (error) {
                console.log("EsOperationHelper.searchAll()");
                console.error("error=" + JSON.stringify(error));
                return callback(error);
            }
            response.hits.hits.forEach(function (hit) {
                hit["_source"]["_id"] = hit["_id"];
                allData.push(hit["_source"]);
            });
            // here change response.hits.total  --->response.hits.total.value  due to 7.2 vesion changes  
            if (response.hits.total.value > allData.length) {
                // ask elasticsearch for the next set of hits from this search
                sails.esClients.scroll({
                    scrollId: response._scroll_id,
                    scroll: scrollTime
                }, getMoreUntilDone);
            } else {
                console.log("EsOperationHelper.searchAll()");
                callback(null, allData);
            }
        });
    },

    /**
     * <p>
     * This method will connect to es clients and get count using given
     * searchQueryJson. In case of invalid searchQueryJson or ES is down it will
     * callback with error same error that ES gives else will return result
     * </p>
     *
     * @param {JSON}
     *            searchQueryJson query json to search from ES
     */
    count: function count(searchQueryJson, callback) {
        console.log("EsOperationHelper.count()");
        console.log("searchQueryJson=" + JSON.stringify(searchQueryJson));
        sails.esClients.count(searchQueryJson, function (error, countResponseFromES) {
            if (error) {
                console.error("error while getting count from ES=" + JSON.stringify(error));
                // callback(error);
                self.createIndexIfMissing(error, searchQueryJson["index"], callback);

            } else {
                callback(null, countResponseFromES.body);
            }
            console.log("EsOperationHelper.count()");
        });
    },
    createIndexIfMissing: function createIndexIfMissing(error, indexName, callback) {
        console.log("EsOperationHelper.createIndexIfMissing()");
        console.error("error=" + (error));
        if (error.toString().indexOf("index_not_found_exception") > -1) {
            console.log("index is missing=" + indexName);
            this.createIndex(indexName, () => { });
            let responseJson = {};
            responseJson[func.jsonCons.FIELD_COUNT] = 0;
            responseJson[func.jsonCons.FIELD_TOTAL_COUNT] = 0;
            responseJson["body"] = [];
            callback(null, responseJson);
        } else {
            callback(error);
        }
        console.log("EsOperationHelper.createIndexIfMissing()");
    },

    createIndex: function createIndex(indexName, callback) {
        console.log("EsOperationHelper.createIndex()");
        console.log("indexName=" + JSON.stringify(indexName));
        sails.esClients.indices.create(indexName, function (error, indexResponseFromES) {
            if (error) {
                console.log("error while creating index in ES=" + JSON.stringify(error));
                callback(error);
            } else {
                console.log("indexing completed sucsessfully=" + JSON.stringify(indexResponseFromES));
                callback(null, indexResponseFromES);
            }
            console.log("EsOperationHelper.createIndex()");
        });
    },

    /**
     * <p>
     * this method will index the queryJson into ES { "index":"some_index",
     * "type":"some_type", "id":"some_id"(optional) "body": {"foo":"bar"} //body
     * json to index into given index and type with id if given }
     * </p>
     *
     * @see getJsonOfBodyIndexTypeNId()
     * @param {JSON}
     *            queryJson
     * @param {Function}
     *            callback
     */
    index: function index(queryJson, callback) {
        console.log("EsOperationHelper.index()");
        console.log("queryJson=" + JSON.stringify(queryJson));
        sails.esClients.index(queryJson, function (error, indexResponseFromES) {
            if (error) {
                console.log("error while indexing in ES=" + JSON.stringify(error));
                callback(error);
            } else {
                callback(null, indexResponseFromES.body);
            }
            console.log("EsOperationHelper.index()");
        });
    },

    bulkIndex: function bulkIndex(bulkQuery, strIndex, callback) {
        console.log("EsOperationHelper.bulkIndex()");
        let self = this;
        sails.esClients.bulk(bulkQuery, function (error, indexResponseFromES) {
            if (error) {
                console.log("error while bulk indexing in ES=" + JSON.stringify(error));
                self.createIndexIfMissing(error, strIndex, callback);
            } else {
                callback(null, indexResponseFromES.body);
            }
            console.log("EsOperationHelper.bulkIndex()");
        });
    }, // bulkIndex
    /**
     * <p>
     * this method will performe delete using query
     * </p>
     *
     * @param {JSON}
     *            queryJson
     * @param {Function}
     *            callback
     */
    deleteByQuery: function deleteByQuery(queryJson, callback) {
        console.log("EsOperationHelper.deleteByQuery()");
        console.log("queryJson=" + JSON.stringify(queryJson));
        let self = this;
        sails.esClients.deleteByQuery(queryJson, function (error, deleteResponseFromES) {
            if (error) {
                console.log("error while deleting in ES=" + JSON.stringify(error));
                self.createIndexIfMissing(error, queryJson["index"], callback);
            } else {
                callback(null, deleteResponseFromES.body);
            }
            console.log("EsOperationHelper.deleteByQuery()");
        });
    },
    /**
     * <p>
     * this method will performe delete using query
     * </p>
     *
     * @param {JSON}
     *            queryJson
     * @param {Function}
     *            callback
     */
    updateByQuery: function updateByQuery(queryJson, callback) {
        console.log("EsOperationHelper.updateByQuery()");
        console.log("queryJson=" + JSON.stringify(queryJson));
        let self = this;
        if (!!queryJson && !queryJson.refresh) queryJson["refresh"] = true // for refresh after updating the data to get new data instantly
        sails.esClients.updateByQuery(queryJson, function (error, updateResponseFromES) {
            console.log("EsOperationHelper.updateByQuery()");
            if (error) {
                console.log("error while updating in ES=" + JSON.stringify(error));
                self.createIndexIfMissing(error, queryJson["index"], callback);
            } else {
                updateResponseFromES = updateResponseFromES.body;
                callback(null, updateResponseFromES);
            }

        });
    },

    /**
     * <p>
     * This method will connect to es clients and search using multiple
     * searchQueryJson. In case of invalid searchQueryJson or ES is down it will
     * callback with error same error that ES gives
     * </p>
     *
     * @param {JSON} searchQueryJson query json to search from ES
     */
    multiSearch: function multiSearch(searchQueryJson, callback) {
        /**
         * multi search query format [[index][query][index][query]]
         */
        console.log("EsOperationHelper.multiSearch()");
        sails.esClients.msearch({
            body: searchQueryJson
        }, function (error, multiSearchResponseFromES) {
            console.log("EsOperationHelper.multiSearch()");
            if (error) {
                console.log("error while getting multiSearch, Error=" + JSON.stringify(error));
                return callback(error);
            } else {
                return callback(null, multiSearchResponseFromES["body"]["responses"]);
            }
        });
    },
    /**
     * <p>
     * this method will perform bulk operations
     * </p>
     * @param {JSON} queryJson
     */
    bulk: function bulk(queryJson, refreshType, callback) {
        console.log("EsOperationHelper.bulk()");
        console.log("queryJson = " + JSON.stringify(queryJson));
        sails.esClients.bulk({
            refresh: refreshType ? refreshType : 'wait_for',
            body: queryJson
        }, function (error, esResponse) {
            console.log("EsOperationHelper.bulk()");
            if (error) {
                console.error("error while perform bulk operations, Error=" + JSON.stringify(error));
                return callback(error);
            } else {
                return callback(null, esResponse["body"]["responses"]);
            }
        });
    },
}; // module exports

function getSearchResponseJson(responseFromES) {
    console.log("EsOperationHelper.getSearchResponseJson()");
    let hitsJsonArray = responseFromES["hits"]["hits"];
    let searchResponseJsonArray = [];
    async.forEach(hitsJsonArray, (hitsJson) => {
        hitsJson["_source"]["_id"] = hitsJson["_id"];
        searchResponseJsonArray.push(hitsJson["_source"]);
    });
    let searchResponseJson = {};
    searchResponseJson["total_count"] = responseFromES["hits"]["total"];
    searchResponseJson["body"] = searchResponseJsonArray;
    console.log("EsOperationHelper.getSearchResponseJson()");
    return searchResponseJson;
}
