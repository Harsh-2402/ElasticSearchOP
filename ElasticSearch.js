var q = require('q');
var elasticsearch = require('elasticsearch');
var client = new elasticsearch.Client({
    host: 'localhost:9200',
    log: 'trace'
});

/**
 * add document is Es
 * 
 * @param key
 *            document key to add
 * @param doc
 *            document data
 * @param index
 *            index name in es
 */
var addDocumentToES = function (key, doc, index) {
    var deffered = q.defer();
    client.index({
        index: index,
        id: key,
        body: doc
    }).then(function (resp) {
        deffered.resolve(resp);
    }, function (error) {
        deffered.reject(error);
    });
    return deffered.promise;
}


/**
 * update document is Es
 * 
 * @param key
 *            document key to add
 * @param doc
 *            document data
 * @param index
 *            index name in es
 */
var updateDocumentInEs = function (key, doc, index) {
    var deffered = q.defer();
    client.update({
        index: index,
        id: key,
        body: {
            doc
            }
    }).then(function (resp) {
        deffered.resolve(resp);
    }, function (error) {
        deffered.reject(error);
    });
    return deffered.promise;
}
var removeDocumentFromEs = function (key,index) {
    var deffered = q.defer();
    client.delete({
        index: index,
        id: key,
    }).then(function (resp) {
        deffered.resolve(resp);
    }, function (error) {
        deffered.reject(error);
    });
    return deffered.promise;
}

var searchDocumentInEs = function (key,index) {
    var deffered = q.defer();
    client.search({
        index: index,
        body:{
            "query": {
                "bool": {
                  "must_not": [
                    {
                      "match": {
                        "isDeleted": true
                      }
                    }
                  ], 
                  "must": [
                    {
                      "match": {
                        "id": key
                      }
                    }
                  ]
                }
              }
        }
    }).then(function (resp) {
        deffered.resolve(resp);
    }, function (error) {
        deffered.reject(error);
    });
    return deffered.promise;
}

var searchDocumentInEsById = function (key,index) {
    var deffered = q.defer();
    client.search({
        index: index,
        body:{
             "query": {
                "bool": {
                    "must": 
                        {"match": {
                            "id": key
                                    }
                        }
      
                        }
  
                        }
        }
    }).then(function (resp) {
        deffered.resolve(resp);
    }, function (error) {
        deffered.reject(error);
    });
    return deffered.promise;
}
var getAllDocFromEs = function (index) {
    var deffered = q.defer();
    client.search({
        index: index,
        body:{
            "size": 50, 
            "query": {
              "match_all": {}
            }
          }
    }).then(function (resp) {
        deffered.resolve(resp);
    }, function (error) {
        deffered.reject(error);
    });
    return deffered.promise;
}


module.exports={
    removeDocumentFromEs:removeDocumentFromEs,
    updateDocumentInEs:updateDocumentInEs,
    addDocumentToES:addDocumentToES,
    searchDocumentInEs:searchDocumentInEs,
    getAllDocFromEs:getAllDocFromEs,
    searchDocumentInEsById:searchDocumentInEsById
}