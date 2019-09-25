from flask import jsonify    
import spacy
import collections
from datetime import datetime
nlp = spacy.load('en_core_web_sm', disable=['parser', 'tagger','textcat','tokenizer']) 

def ner_api(request):
    start_time = datetime.now()
    msg = request.get_json()
    out={}
    out["Owner"]=msg["Owner"]
    out["Source"]=msg["Source"]
    data=msg["Data"]
    columns=[]
    for column_name in list(data.keys()):
        column={}
        column["ColumnName"]=column_nameQ
        entities=[]

        doc=nlp(repr(data[column_name]))
        for ent in doc.ents:
            entities.append(ent.label_)

        #for value in data[column_name]:
        #    doc=nlp(value.__repr__())
        #    entities = [element.label_ for element in doc.ents]
        
        entities_dict={k: v / len(entities) for k, v in collections.Counter(entities).items()}
        column["NEREntities"]=entities_dict
        columns.append(column)

    out["Columns"]=columns
    out["TimeStamp"]=start_time.strftime('%Y-%m-%d %H:%M:%S.%f')
    out["ElapsedTime"]=(datetime.now() - start_time).total_seconds() 

    return jsonify(out)