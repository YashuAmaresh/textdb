import { Data } from './data';

let defaultData = {
    operators: {
        operator1: {
            top: 20,
            left: 20,
            properties: {
                title: 'Operator 1',
                inputs: {},
                outputs: {
                    output_1: {
                        label: 'Output 1',
                    }
                }
            }
        },
        operator2: {
            top: 80,
            left: 300,
            properties: {
                title: 'Operator 2',
                inputs: {
                    input_1: {
                        label: 'Input 1',
                    },
                    input_2: {
                        label: 'Input 2',
                    },
                },
                outputs: {}
            }
        },
    },
    links: {
        link_1: {
            fromOperator: 'operator1',
            fromConnector: 'output_1',
            toOperator: 'operator2',
            toConnector: 'input_2',
        },
    }
};

let keywordMatcher = {
    top: 20,
    left: 20,
    properties: {
        title: 'KeywordMatcher',
        inputs: {
            input_1: {
                label: 'Input (:i)',
            }
        },
        outputs: {
            output_1: {
                label: 'Output (:i)',
            }
        },
        attributes: {
            "operatorType": "KeywordMatcher",
            "query": "keyword",
            "attributes": ["attr1", "attr2"],
            "luceneAnalyzer": "standard",
            "matchingType": "conjunction",
            "addSpans": true
        }
    }
};

let regexMatcher = {
  top : 20,
  left : 20,
  properties : {
    title : 'RegexMatcher',
    inputs : {
      input_1 : {
        label : 'Input(:i)',
      }
    },
    outputs : {
      output_1 : {
        label : 'Output (:i)',
      }
    },
    attributes : {
        "operatorType": "RegexMatcher",
        "regex": "regex",
        "attributes": ["attr1", "attr2"],
        "regexIgnoreCase": false
    }
  }
};

let dictionaryMatcher = {
  top : 20,
  left : 20,
  properties : {
    title : 'DictionaryMatcher',
    inputs : {
      input_1 : {
        label : "Input(:i)",
      }
    },
    outputs :{
      output_1 : {
        label : "Output(:i)",
      }
    },
    attributes :  {
        "operatorType": "DictionaryMatcher",
        "dictionaryEntries": ["entry1", "entry2"],
        "attributes": ["attr1", "attr2"],
        "luceneAnalyzer": "standard",
        "matchingType": "conjunction"
    }
  }
}

let FuzzyMatcher = {
  top : 20,
  left : 20,
  properties : {
    title : "FuzzyTokenMatcher",
    inputs : {
      input_1 : {
        label : "Input(:i)",
      }
    },
    outputs : {
      output_1 : {
        label : "Output(:i)",
      }
    },
    attributes : {
        "operatorType": "FuzzyTokenMatcher",
        "query": "token1 token2 token3",
        "attributes": ["attr1", "attr2"],
        "luceneAnalyzer": "standard",
        "thresholdRatio": 0.8
    }
  }
}

let nlpEntity = {
  top : 20,
  left : 20,
  properties : {
    title : 'NlpEntity',
    inputs : {
      input_1 : {
        label : 'Input(:i)',
      }
    },
    outputs : {
      output_1 : {
        label : "Output (:i)",
      }
    },
    attributes : {
        "operatorType": "NlpEntity",
        "nlpEntityType": "location",
        "attributes": ["attr1", "attr2"]
    }
  }
}

let nlpSentiment = {
  top : 20,
  left : 20,
  properties : {
    title : 'nlpSentiment',
    inputs : {
      input_1 : {
        label : 'Input(:i)',
      }
    },
    outputs : {
      output_1 : {
        label : "Output (:i)",
      }
    },
    attributes : {
        "operatorType": "NlpSentiment",
        "attribute": "inputAttr",
        "resultAttribute": "resultAttr"
    }
  }
}

let regexSplit = {
  top : 20,
  left : 20,
  properties : {
    title : 'RegexSplit',
    inputs : {
      input_1 : {
        label : "Input (:i)",
      }
    },
    outputs : {
      output_1 : {
        label : "Output (:i)",
      }
    },
    attributes : {
        "operatorType": "RegexSplit",
        "splitRegex": "regex",
        "splitAttribute": "attr1",
        "splitType": "standalone"
    }
  }
}

let sampler = {
  top : 20,
  left : 20,
  properties : {
    title : 'RegexSplit',
    inputs : {
      input_1 : {
        label : "Input (:i)",
      }
    },
    outputs : {
      output_1 : {
        label : "Output (:i)",
      }
    },
    attributes : {
        "operatorType": "Sampler",
        "sampleSize": 10,
        "sampleType": "firstk"
    }
  }
}

let Projection = {
  top : 20,
  left : 20,
  properties : {
    title : 'Projection',
    inputs : {
      input_1 : {
        label : "Input (:i)",
      }
    },
    outputs : {
      output_1 : {
        label : "Output (:i)",
      }
    },
    attributes : {
        "operatorType": "Projection",
        "attributes": ["attr1", "attr2"]
    }
  }
}

let scanSource = {
  top : 20,
  left : 20,
  properties : {
    title : 'ScanSource',
    inputs : {
      input_1 : {
        label : "Input (:i)",
      }
    },
    outputs : {
      output_1 : {
        label : "Output (:i)",
      }
    },
    attributes : {
        "operatorType": "ScanSource",
        "tableName": "promed"
    }
  }
}

let keywordSource = {
  top : 20,
  left : 20,
  properties : {
    title : 'KeywordSource',
    inputs : {
      input_1 : {
        label : "Input (:i)",
      }
    },
    outputs : {
      output_1 : {
        label : "Output (:i)",
      }
    },
    attributes : {
        "operatorType": "KeywordSource",
        "query": "keyword",
        "attributes": ["attr1", "attr2"],
        "luceneAnalyzer": "standard",
        "matchingType": "conjunction",
        "tableName": "tableName",
        "addSpans": false
    }
  }
}


let DictionarySource = {
  top : 20,
  left : 20,
  properties : {
    title : 'DictionarySource',
    inputs : {
      input_1 : {
        label : "Input (:i)",
      }
    },
    outputs : {
      output_1 : {
        label : "Output (:i)",
      }
    },
    attributes : {
        "operatorType": "DictionarySource",
        "dictionaryEntries": ["entry1", "entry2"],
        "attributes": ["attr1", "attr2"],
        "luceneAnalyzer": "standard",
        "matchingType": "conjunction",
        "tableName": "tableName"
    }
  }
}

let RegexSource = {
  top : 20,
  left : 20,
  properties : {
    title : 'RegexSource',
    inputs : {
      input_1 : {
        label : "Input (:i)",
      }
    },
    outputs : {
      output_1 : {
        label : "Output (:i)",
      }
    },
    attributes : {
        "operatorType": "RegexSource",
        "regex": "regex",
        "attributes": ["attr1", "attr2"],
        "regexIgnoreCase": false,
        "tableName": "tableName",
        "regexUseIndex": true
    } 
  }
}

let FuzzyTokenSource = {
  top : 20,
  left : 20,
  properties : {
    title : 'FuzzyTokenSource',
    inputs : {
      input_1 : {
        label : "Input (:i)",
      }
    },
    outputs : {
      output_1 : {
        label : "Output (:i)",
      }
    },
    attributes : {
        "operatorType": "FuzzyTokenSource",
        "query": "token1 token2 token3",
        "attributes": ["attr1", "attr2"],
        "luceneAnalyzer": "standard",
        "thresholdRatio": 0.8,
        "tableName": "tableName"
    }
  }
}

let characterDistanceJoin = {
  top : 20,
  left : 20,
  properties : {
    title : 'CharacterDistanceJoin',
    inputs : {
      input_1 : {
        label : 'Input (:i)',
      },
      input_2 : {
        label : "Input 2",
      }
    },
    outputs : {
      output_1 : {
        label : "Output (:i)",
      }
    },
    attributes : {
        "operatorType": "JoinDistance",
        "innerAttribute": "attr1",
        "outerAttribute": "attr1",
        "spanDistance": 100
    }
  }
}

let similarityJoin = {
  top : 20,
  left : 20,
  properties : {
    title : 'Join',
    inputs : {
      input_1 : {
        label : 'Input (:i)',
      },
      input_2 : {
        label : "Input 2",
      }
    },
    outputs : {
      output_1 : {
        label : "Output (:i)",
      }
    },
    attributes : {
        "operatorType": "SimilarityJoin",
        "innerAttribute": "attr1",
        "outerAttribute": "attr1",
        "similarityThreshold": 0.8
    }
  }
}



let result = {
  top : 20,
  left : 20,
  properties : {
    title : 'View Results',
    inputs : {
      input_1 : {
        label : "Input (:i)",
      }
    },
    outputs : {
      output_1 : {
        label : "Output (:i)",
      }
    },
    attributes : {
        "operatorType": "ViewResults",
        "limit": 2147483647,
        "offset": 0
    }
  }
}

export const DEFAULT_DATA: Data[] = [
    {id: 1, jsonData: {}}
];
// DictionarySource, RegexSource, FuzzyTokenSource

export const DEFAULT_MATCHERS: Data[] = [
    {id: 0, jsonData: regexMatcher},
    {id: 1, jsonData: keywordMatcher},
    {id: 2, jsonData: dictionaryMatcher},
    {id: 3, jsonData: FuzzyMatcher},
    {id: 4, jsonData: nlpEntity},
    {id: 5, jsonData: nlpSentiment},
    {id: 6, jsonData: regexSplit},
    {id: 7, jsonData: sampler},
    {id: 8, jsonData: Projection},
    {id: 9, jsonData: scanSource},
    {id: 10, jsonData: keywordSource},
    {id: 11, jsonData: DictionarySource},
    {id: 12, jsonData: RegexSource},
    {id: 13, jsonData: FuzzyTokenSource},
    {id: 14, jsonData: characterDistanceJoin},
    {id: 15, jsonData: similarityJoin},
    {id: 16, jsonData: result},
];
