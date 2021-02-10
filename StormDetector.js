function OnUpdate(doc, meta) {
    log('docId', meta.id);
    
    if(!doc["wind_speed"]) {
        log('1. NO doc["wind_speed"] so I stop here');
        return;
    }
    
    log('2. doc["wind_speed"] exists, let s continue');
    
    var wind_alert_doc = {};
    wind_alert_doc["wind_speed"] = doc["wind_speed"];
    wind_alert_doc["location"] = doc["location"];
    wind_alert_doc["type"] = "WindAlert";
    
    log('3. wind_alert_doc["wind_speed"]', wind_alert_doc["wind_speed"])
    
    if(doc["wind_speed"] >= 32.7) {
        wind_alert_doc["status"] = "Hurricane";
    } else if (doc["wind_speed"] >= 28.5) {
        wind_alert_doc["status"] = "Violent storm";
    } else if (doc["wind_speed"] >= 24.5) {
        wind_alert_doc["status"] = "Storm";
    } else {
        wind_alert_doc["status"] = "No storm";
    }
    
    log('4. wind_alert_doc["status"]', wind_alert_doc["status"]);
    
    var new_doc_id = "WindAlert::" + doc["location"];
    
    log('5. new_doc_id', new_doc_id);
    log('6. wind_alert_doc', wind_alert_doc);
    
    tgt[new_doc_id] = wind_alert_doc;
    
    log('7. tgt[new_doc_id] has been stored: ', tgt[new_doc_id]);
}

function OnDelete(meta) {
}
