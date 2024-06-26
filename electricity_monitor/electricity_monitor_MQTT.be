#-
 - 
 -#
 import json
 import persist
 import mqtt
 import string 
 import backoff_actuator
 import activate_actuator

 var sensors = {
 'missing':{'parent':'Binary','tag':'Missing'},
 'Sum_over':{'parent':'Binary','tag':'SumOvr'},
 'Sum_under':{'parent':'Binary','tag':'SumUnd'},
 'Sum_over_gen':{'parent':'Binary','tag':'SumGenOvr'},
 'Sum_o_lmt':{'parent':'Power','tag':'SumOvrLmt'},
 'Sum_u_lmt':{'parent':'Power','tag':'SumUndLmt'},
 'Sum_o_g_lmt':{'parent':'Power','tag':'SumGenOvrLmt'},
 'AvgPwr_over':{'parent':'Binary','tag':'AvgPwrOvr'}, 
 'AvgPwr_under':{'parent':'Binary','tag':'AvgPwrUnd'},
 'power_sum':{'parent':'Power','tag':'PwrSum'},
 'AvgPwr_Active':{'parent':'Power','tag':'AvgPwr'},
 'Ph1_over':{'parent':'Binary','tag':'Ph1Ovr'},
 'Ph2_over':{'parent':'Binary','tag':'Ph2Ovr'},
 'Ph3_over':{'parent':'Binary','tag':'Ph3Ovr'},
 'Ph1_over_gen':{'parent':'Binary','tag':'Ph1GenOvr'},
 'Ph2_over_gen':{'parent':'Binary','tag':'Ph2GenOvr'},
 'Ph3_over_gen':{'parent':'Binary','tag':'Ph3GenOvr'},
 'Ph1_under':{'parent':'Binary','tag':'Ph1Und'},
 'Ph2_under':{'parent':'Binary','tag':'Ph2Und'},
 'Ph3_under':{'parent':'Binary','tag':'Ph3Und'}, 
 'Ph1':{'parent':'Power','tag':'Ph1'},
 'Ph2':{'parent':'Power','tag':'Ph2'},
 'Ph3':{'parent':'Power','tag':'Ph3'},
 'Ph2_u_lmt':{'parent':'Power','tag':'Ph2UndLmt'},
 'Ph3_u_lmt':{'parent':'Power','tag':'Ph3UndLmt'},
 'Ph1_u_lmt':{'parent':'Power','tag':'Ph1UndLmt'}, 
 'power_sum_delta':{'parent':'Power','tag':'PwrSumDlt'}, 
 'Ph1_o_g_lmt':{'parent':'Power','tag':'Ph1GenOvrLmt'},
 'Ph2_o_g_lmt':{'parent':'Power','tag':'Ph2GenOvrLmt'},
 'Ph3_o_g_lmt':{'parent':'Power','tag':'Ph3GenOvrLmt'},
 'Ph3_gen':{'parent':'Power','tag':'Ph3Gen'},
 'Ph2_gen':{'parent':'Power','tag':'Ph2Gen'},
 'Ph1_gen':{'parent':'Power','tag':'Ph1Gen'},
 'Ph1_o_lmt':{'parent':'Power','tag':'Ph1OvrLmt'},
 'Ph2_o_lmt':{'parent':'Power','tag':'Ph2OvrLmt'},
 'Ph3_o_lmt':{'parent':'Power','tag':'Ph3OvrLmt'} }

 var stubs = {
    'values':{'Ph1':['Ph1','Ph1_gen'],
                'Ph2':['Ph2','Ph2_gen'],
                'Ph3':['Ph3','Ph3_gen']},
    'limits':{'Ph1':['Ph1_under','Ph1_over','Ph1_over_gen','Ph1_o_lmt','Ph1_u_lmt','Ph1_o_g_lmt'],
            'Ph2':['Ph2_under','Ph2_over','Ph2_over_gen','Ph2_o_lmt','Ph2_u_lmt','Ph2_o_g_lmt'],
            'Ph3':['Ph3_under','Ph3_over','Ph3_over_gen','Ph3_o_lmt','Ph3_u_lmt','Ph3_o_g_lmt'],
            'Sum':['Sum_under','Sum_over_gen','Sum_over','Sum_o_g_lmt','Sum_o_lmt','Sum_u_lmt']}
 }

  #-A command supporting setup of electricity monitor driver. It takes a JSON defining electricoty MQTT subscriptions in a list,
 -#
 def electricity_monitor_MQTT_setup(cmd, idx, payload, payload_json)
    var emQ_set
    var emQ_set_topic
    var no_restart
    var resp = map()
    var preset_relays = {'All off':false} 
    
    # parse payload
    if payload_json != nil 
        no_restart = payload_json.find('no_restart')
        emQ_set = payload_json.find('emQ_set')
        emQ_set_topic = payload_json.find('emQ_set_topic')   
        if emQ_set == nil
            emQ_set = map()
        end
        if emQ_set != nil           
            if emQ_set.contains('relays')
                for r: emQ_set['relays']
                    if r.contains('name')
                        if preset_relays.contains(r['name'])
                            preset_relays[r['name']] = true
                        end
                    end
                end
            else
                emQ_set['relays'] = list()            
            end            
            for p: preset_relays.keys()
                if !preset_relays[p]
                    emQ_set['relays'].push({'name':p})
                end
            end                                 
        end
    else
        resp['error'] = 'Malformed JSON or no setup parameter'         
        tasmota.resp_cmnd(resp)
        return   
    end        
    if emQ_set_topic != nil
        persist.emQ_set_topic = emQ_set_topic
        resp['emQ_set_topic'] = emQ_set_topic
    end   
    if emQ_set != nil
        persist.emQ_set = emQ_set
        resp['emQ_set'] = emQ_set        
    end     
  
    # save to _persist.json
    persist.save() 
    #return data as they were saved to persist.
    tasmota.resp_cmnd(resp)
    # report the command as successful    
    if !no_restart
        tasmota.cmd('Restart 1')
    end

end

var electricity_monitor_MQTT = module('electricity_monitor_MQTT')

#-ElectricityMonitorMQTT is a class supporting driving electricity consumers by reading MQTT values of electricy power meter 
on the boundary of microgrid with controlled output. 
-#
class ElectricityMonitorMQTT     
    var data
    var actuators
    var relay_idents 
    var topics        
    var set_timestamp  
    var energy_history 

    var error
  
    def init()     
        self.data = map()            
        self.relay_idents = map()           
        self.actuators = map()   
        self.topics = map()        
        self.error = list()
        self.set_timestamp = tasmota.millis()    
        self.energy_history = map()                 
        self.subscribe_mqtt()        
        self.prep_virt_relays()
        self.prep_actuators()
        self.prep_topics()        
    end

    def prep_virt_relays()
        if persist.has('emQ_set') && persist.emQ_set.contains('relays')
            for rel: persist.emQ_set['relays']
                var ident    
                if rel.contains('ident')
                    ident = rel['ident']
                    if ident+1 > tasmota.global.devices_present
                        tasmota.global.devices_present = ident +1
                    end
                else
                    ident = tasmota.global.devices_present
                    rel['ident'] = ident
                    tasmota.global.devices_present += 1
                end  
                if rel.contains('last_val') && ident
                    var lv = rel['last_val']                                                    
                    tasmota.set_power(ident,lv)
                end
                if rel.contains('name')
                    var nm = rel['name']
                    self.relay_idents[nm] = ident    
                    if ident < 8                                    
                        var cmnd = string.format('FriendlyName%s %s',ident+1,rel['name'])
                        tasmota.cmd(cmnd)
                    end
                    var cmnd2 = string.format('WebButton%s %s',ident+1,rel['name'])
                    tasmota.cmd(cmnd2)
                end
            end
        end
    end  

    def load_actuator(act)
        if act.contains('name')
            var nm = act['name']
            var alloff_id                    
            if self.relay_idents.contains('All off')
                alloff_id = self.relay_idents['All off']
            end                    
            if act.contains('type')
                var a_tp = act['type']
                if a_tp == 'backoff'
                    self.actuators[nm] = backoff_actuator.actuator(act,alloff_id) 
                    log(string.format("emQ: Actuator %s of type %s loaded.",nm,a_tp))                          
                elif a_tp == 'activate'  
                    self.actuators[nm] = activate_actuator.actuator(act,alloff_id)      
                    log(string.format("emQ: Actuator %s of type %s loaded.",nm,a_tp))                       
                else
                    log(string.format("emQ: Actuator %s has unknown type %s .",nm,a_tp))                                                
                end
            else
                log(string.format("emQ: Actuator %s doas not have a type.",nm))   
            end    
        else
            log("emQ: We found an actuator without a name. It can not be loaded.")
        end
    end

    def prep_actuators()
        if persist.has('emQ_set') && persist.emQ_set.contains('actuators')            
            var actuator_cnt = size(persist.emQ_set['actuators'])
            for act: persist.emQ_set['actuators']                
                self.load_actuator(act)
            end
        end
    end

    def load_meter(met)
        if met.contains('name')  
            var nm = met['name'] 
            var value_keys = met.find('value_keys')          
            var limits = met.find('limits')
            var energy_keys = met.find('energy_keys')
            var minutes_for_average = met.find('minutes_for_average')
            var publish_period = met.find('publish_period')
            var report_delay = met.find('report_delay')                    
            var topic = met.find('topic')
            if topic != nil                        
                if !self.topics.contains(topic)
                    self.topics[topic] = map()
                end
                if publish_period != nil
                    self.topics[topic]['period'] = publish_period
                end
                if report_delay != nil
                    self.topics[topic]['report_delay'] = report_delay                            
                end
                if value_keys != nil
                    self.topics[topic]['value_keys'] = value_keys
                end
                if energy_keys != nil
                    self.topics[topic]['energy_keys'] = energy_keys
                end  
                if limits != nil
                    self.topics[topic]['limits'] = limits
                end    
                if minutes_for_average != nil
                    self.topics[topic]['minutes_for_average'] = minutes_for_average
                    log(string.format("emQ: Got time for sliding average length from setup: %d min",minutes_for_average))                            
                end            
                self.topics[topic]["name"] = nm
                self.topics[topic]["subscription"] = tasmota.millis()
                mqtt.subscribe(topic)
                log('emQ: Subscribed ' + topic)  
            else
                log(string.format("emQ: Meter %s has no topic set.",nm))
            end                    
        end
    end

    def prep_topics()
        if persist.has('emQ_set') && persist.emQ_set.contains('meters') 
            for met: persist.emQ_set['meters'] 
                self.load_meter(met)
            end
        end
    end

    def check_holiday(current_time)        
        if persist.has('emQ_set') && persist.emQ_set.contains('holidays')
            var holidays = persist.emQ_set['holidays']
            for hldy : holidays
                var found = false
                for hldy_prt_nm : hldy.keys()
                    if current_time.contains(hldy_prt_nm)
                        if current_time[hldy_prt_nm] == hldy[hldy_prt_nm]
                            found = true
                        else
                            found = false
                            break                                                    
                        end
                    else
                        found = false
                        break
                    end
                end
                if found == true
                    return true
                else
                    continue
                end
            end
        end
        return false
    end

    def get_time_slot()
        var timer_tp_translation = {'h':'hour','m':'month','dow':'weekday'}
        var ret = map()
        var k = tasmota.rtc('local')
        var current_time = tasmota.time_dump(k)
        if persist.has('emQ_set')
            ret["hol"] =  self.check_holiday(current_time)                
            if persist.emQ_set.contains('time_slot_defs')
                var time_slot_defs = persist.emQ_set['time_slot_defs']                               
                for timer_tp: time_slot_defs.keys()                    
                    if timer_tp_translation.contains(timer_tp)
                        var tm_nm = timer_tp_translation[timer_tp]
                        if current_time.contains(tm_nm)
                            var val = current_time[tm_nm]
                            for slot_nm: time_slot_defs[timer_tp].keys()
                                for vector: time_slot_defs[timer_tp][slot_nm]
                                    var start = vector[0]
                                    var duration = vector[1]
                                    var diff = val - start                                    
                                    if diff >= 0 && diff < duration
                                        ret[timer_tp] = slot_nm
                                    end
                                end
                            end
                        end
                    end
                end                
            end

        end
        return ret
    end

    def get_limit_slot(time_slot)
        var ret = map()        
        if persist.has('emQ_set')
            if persist.emQ_set.contains('limit_slot_defs')                
                var limit_slot_defs = persist.emQ_set['limit_slot_defs']   
                var found = false                                  
                for limit_slot: limit_slot_defs.keys()                                                                            
                    for time_slot_i: limit_slot_defs[limit_slot]                        
                        var eq = false                        
                        for time_tp: time_slot_i.keys()
                            if time_slot.contains(time_tp)
                                if time_slot_i[time_tp] == time_slot[time_tp]
                                    eq = true
                                else                                    
                                    eq = false
                                    break
                                end
                            else                                
                                eq = false
                                break
                            end
                        end
                        if eq
                            found = true
                            break
                        end
                    end
                    if found
                        ret['slot'] = limit_slot
                        break
                    end
                end                
            end

        end
        return ret

    end

    def set_consumers(name)                      
        if persist.has('emQ_set')   
            var future = map()         
            for k :self.actuators.keys()
                var act = self.actuators[k]
                if act.control_id == name
                    if self.data != nil
                        var r_v = act.control_actuator(self.data,future)                
                        if r_v != nil
                            if !future.contains(act.control_value)
                                future[act.control_value] = 0
                            end
                            future[act.control_value] += r_v
                        end
                    end
                end
            end
        end
    end

    def set_power_handler(cmd, idx)
        var new_state = tasmota.get_power()
        if persist.has('emQ_set') && persist.emQ_set.contains('relays')
            for rel: persist.emQ_set['relays'] 
                var nm 
                if rel.contains('name')
                    nm = rel['name']   
                end                 
                var ident = rel['ident']                
                if ident <= size(new_state)
                    var st = new_state[ident]    
                    var lv = nil
                    if rel.contains('last_val')
                        lv = rel['last_val']
                    end
                    if lv != st
                        rel['last_val'] = st                                                                                                     
                    end
                end           
            end
        end
    end 

    def subscribe_mqtt() 
        if !persist.has('emQ_set_topic') return nil end       
        if !persist.emQ_set_topic return nil end  #- exit if not initialized -#        
        mqtt.subscribe(persist.emQ_set_topic)  
        log('emQ: Subscribed ' + persist.emQ_set_topic)            
    end

    #- trigger a read every second -#
    def every_second()
        if !persist.has('emQ_set') return nil end  #- exit if not initialized -#  
        if !self.data return nil end  #- exit if no data -# 
        var periods = map()
        if self.topics != nil
            for t: self.topics.keys()
                var tpk = self.topics[t]
                var nm = "Unknown"
                if tpk.contains("name")
                    nm = tpk["name"]
                end
                if tpk.contains('period')   
                    var per = tpk['period']
                    periods[nm] = per
                end
            end
        end

        var max_t = 0
        var current = tasmota.millis()  
        var any_missing = False
        for k: self.data.keys()            
            var val_map = self.data[k]            
            var time_delta = 1000000000            
            for kk: val_map.keys()                
                if kk=='last' 
                    var val = val_map[kk]
                    time_delta = current - val
                    if val > max_t
                        max_t = val
                    end
                end
            end
            val_map["time_delta"] = time_delta
            if periods.contains(k)
                val_map["missing"] = (time_delta > 2*periods[k]*1000) 
            end
            if val_map.contains('missing')
                any_missing = any_missing || val_map['missing']
            end
        end        
        if max_t > self.set_timestamp                        
            self.set_timestamp = tasmota.millis()             
        end
        self.data['time_slot'] = self.get_time_slot()
        self.data['limit_slot'] = self.get_limit_slot(self.data['time_slot'])
    end
  
    #- display sensor value in the web UI -#
    def web_sensor()
        if !self.data return nil end  #- exit if not initialized -#   
        var msg=''
        for k: self.data.keys()
            msg += '{t}'
            if k == 'Time' continue 
            elif k=='time_slot'
                var vm = self.data[k]
                for nm:vm.keys()
                    if nm == 'hol'
                        msg += string.format('{s}%s %s{m}%s{e}',k,nm,vm[nm]?'True':'False')
                    else
                        msg += string.format('{s}%s %s{m}%s{e}',k,nm,vm[nm]) 
                    end
                end
                continue
            elif k=='limit_slot'
                var vm = self.data[k]
                if vm.contains('slot')
                    msg += string.format('{s}%s{m}%s{e}',k,vm['slot'])                 
                end
                continue
            end    
            msg += '{t}'        
            var val_map = self.data[k]
            for kk: val_map.keys()
                var val = val_map[kk]
                if kk == 'missing' ||
                    kk == 'Sum_over' ||
                    kk == 'AvgPwr_over' ||
                    kk == 'Ph1_over' ||
                    kk == 'Ph2_over' ||
                    kk == 'Ph3_over' ||
                    kk == 'Sum_over_gen' ||
                    kk == 'Ph1_over_gen' ||
                    kk == 'Ph2_over_gen' ||
                    kk == 'Ph3_over_gen' || 
                    kk == 'Sum_under' ||
                    kk == 'AvgPwr_under' ||
                    kk == 'Ph1_under' ||
                    kk == 'Ph2_under' || 
                    kk == 'Ph3_under'
                    msg += string.format('{s}%s %s{m}%s{e}',k,kk,val?'True':'False')   
                elif kk=='last' || kk=='time_delta' || kk=='Energy_d_tm'
                    msg += string.format('{s}%s %s{m}%s ms{e}',k,kk,val)  
                elif kk=='report_delay'
                    msg += string.format('{s}%s %s{m}%s s{e}',k,kk,val)  
                elif kk=='Energy_d_Active'
                    msg += string.format('{s}%s %s{m}%s kWh{e}',k,kk,val)                   
                else
                    msg += string.format('{s}%s %s{m}%.2f W{e}',k,kk,val)
                end
            end                 
        end  
        for err:self.error     
            msg += string.format('{s}%s{e}',err)
        end
        tasmota.web_send_decimal(msg)
    end    

    #- add sensor value to teleperiod -#
    def json_append()
        if !self.data return nil end  #- exit if not initialized -#   
        if !self.data.contains('time_slot')     
            self.data['time_slot'] = self.get_time_slot()
            self.data['limit_slot'] = self.get_limit_slot(self.data['time_slot'])
        end
        var data_stubs = map()
        if self.topics != nil
            for t: self.topics.keys()
                var tpk = self.topics[t]
                var nm = "Unknown"
                if tpk.contains("name")
                    nm = tpk["name"]
                end
                var is_eks = tpk.contains("energy_keys")
                var vks = map()
                if tpk.contains("value_keys")
                    vks = tpk["value_keys"]
                end
                var lmts = map()
                if tpk.contains("limits")
                    lmts = tpk["limits"]
                end
                if !self.data.contains(nm)
                    data_stubs[nm] = map()
                    data_stubs[nm]['Binary'] = map()
                    data_stubs[nm]['Power'] = map()
                    var nms = ['missing','power_sum','power_sum_delta']
                    for vk:vks.keys()
                        if stubs['values'].contains(vk)
                            nms = nms + stubs['values'][vk]
                        end
                    end
                    for lmt:lmts.keys()
                        if stubs['limits'].contains(lmt)
                            nms = nms + stubs['limits'][lmt]
                        end
                    end
                    if is_eks
                        nms = nms + ['AvgPwr_over','AvgPwr_under','AvgPwr_Active']                        
                    end
                    for nn:nms
                        if sensors.contains(nn)
                            var sens_def = sensors[nn]
                            if sens_def.contains('tag')
                                var tg = sens_def['tag']
                                if sens_def.contains('parent')
                                    var prnt = sens_def['parent']
                                    if prnt == 'Binary'
                                        data_stubs[nm][prnt][tg] = 'OFF'
                                    else
                                        data_stubs[nm][prnt][tg] = 0
                                    end
                                else
                                    data_stubs[nm][tg] = 'Unknown'
                                end  
                            else
                                continue
                            end  
                            
                        end
                    end
                end
            end
        end
        var msg = ''
        var vals = map()
        for k: self.data.keys()
            if k == 'Time' continue
            elif k=='time_slot'
                var vm = self.data[k]                
                for nm:vm.keys()
                    if nm == 'hol'                        
                        vals['Hldy'] = vm[nm]?'Yes':'No' 
                    else                        
                        vals[nm] = vm[nm] 
                    end
                end                
                continue
            elif k=='limit_slot'
                var vm = self.data[k]
                if vm.contains('slot')
                    vals["LmtSlt"] = vm['slot']
                end                
                continue
            end    
            var sens = map()
            sens['Binary'] = map()  
            sens['Power'] = map()              
            var val_map = self.data[k]    
            sens['Id'] = k                  
            for kk: val_map.keys()     
                var val = val_map[kk]
                if sensors.contains(kk)
                    var sens_def = sensors[kk]
                    if sens_def.contains('tag')
                        var tg = sens_def['tag']
                        if sens_def.contains('parent')
                            var prnt = sens_def['parent']
                            if prnt == 'Binary'
                                sens[prnt][tg] = val?'ON':'OFF'
                            else
                                sens[prnt][tg] = val
                            end
                        else
                            sens[tg] = val
                        end  
                    else
                        continue
                    end  
                else
                    continue
                end                        
            end   
            msg += string.format(',"%sMeter":%s', k,json.dump(sens))         
        end  
        for stb: data_stubs.keys()
            msg += string.format(',"%sMeter":%s', stb,json.dump(data_stubs[stb]))
        end
        msg += string.format(',"TCpsl":%s',json.dump(vals))                  
        tasmota.response_append(msg)
    end

    def mqtt_data(topic, idx, data, databytes)
        var ret = false
        if self.mqtt_setup(topic, idx, data, databytes) return true end
        if size(self.actuators)==0 && size(self.topics)==0 return false end  
        for k :self.actuators.keys()
            var act = self.actuators[k]
            if act.mqtt_data(topic, idx, data, databytes) 
                return true
            end
        end
        if !self.topics.contains(topic) return false end                    
        try
            var payload_json = json.load(data)
            if payload_json != nil 
                var tpk = self.topics[topic]
                var nm = "Unknown"
                if tpk.contains("name")
                    nm = tpk["name"]
                end
                var pwr_data = map()
                if tpk.contains('energy_keys')
                    var ev = tpk['energy_keys']                                             
                    var hour_to_ms = 3600000
                    var mins_4_avg = tpk.find('minutes_for_average')                    
                    if mins_4_avg == nil mins_4_avg=15 end
                    var energy_window = mins_4_avg * hour_to_ms / 60                    
                    var val_map = map()                    
                    var ts = tasmota.millis()
                    var time_dlt = 0
                    if !self.energy_history.contains(nm)
                        self.energy_history[nm] = list()
                    end                    
                    #log(string.format("History size: %d",size(self.energy_history[nm])))
                    if size(self.energy_history[nm]) >0 
                        time_dlt = ts - self.energy_history[nm][0]['tm']
                    end
                    val_map['tm'] = ts                            
                    for e: ev.keys()                    
                        var energy = payload_json.find(e)                        
                        #log(string.format("Energy: %s %d %d %d",e,energy,time_dlt,energy_window))
                        if energy != nil
                            var et = ev[e]                           
                            val_map[et] = energy
                        end                        
                    end
                    if time_dlt >= energy_window
                        var prev = self.energy_history[nm].pop(0)
                        for tt:prev.keys()
                            pwr_data['Energy_d_' + tt] = val_map[tt]-prev[tt]
                        end
                        var tm = pwr_data.find('Energy_d_tm')
                        var c = pwr_data.find('Energy_d_Active')    
                        #log(string.format('diff: %d %d',tm,c))                    
                        pwr_data['AvgPwr_Active'] = (1000*c*hour_to_ms)/tm
                    end
                    self.energy_history[nm].push(val_map)
                end
                if tpk.contains('value_keys')
                    var kv = tpk['value_keys']
                    var lmt = nil
                    if tpk.contains('limits')
                        lmt = tpk['limits']                       
                    end
                    var limit_slot = nil
                    var limit_slot_data = self.data.find('limit_slot')
                    if limit_slot_data != nil
                        if limit_slot_data.contains('slot')
                            limit_slot = limit_slot_data['slot']   
                        end
                    end
                    var sum = 0
                    var delta_sum = 0
                    for k: kv.keys()
                        var value = payload_json.find(k)
                        if value != nil
                            var t = kv[k]
                            if lmt != nil && lmt.contains(t) 
                                var all = lmt[t].find('*')
                                var slot_lmt = nil
                                if limit_slot != nil
                                    slot_lmt = lmt[t].find(limit_slot)                                                        
                                end
                                var local_lmt = nil
                                if all != nil
                                    local_lmt = all
                                elif slot_lmt != nil
                                    local_lmt = slot_lmt
                                end
                                if local_lmt != nil
                                    if  local_lmt.contains("max")
                                        var max = local_lmt["max"]
                                        pwr_data[t + '_o_lmt'] = max
                                        if value > max
                                            pwr_data[t + '_over']=true
                                        else
                                            pwr_data[t + '_over']=false 
                                        end
                                    end
                                    if local_lmt.contains("max_gen")
                                        var max_gen = local_lmt["max_gen"]
                                        pwr_data[t + '_o_g_lmt'] = max_gen
                                        if value < 0 && (-1*value) > max_gen
                                            pwr_data[t + '_over_gen']=true
                                        else
                                            pwr_data[t + '_over_gen']=false 
                                        end
                                    end
                                    if local_lmt.contains("min")
                                        var min = local_lmt["min"]     
                                        pwr_data[t + '_u_lmt'] = min                                                                   
                                        if value < min
                                            pwr_data[t + '_under']=true
                                        else
                                            pwr_data[t + '_under']=false 
                                        end
                                    end
                                end
                            end
                            if pwr_data.contains(t)
                                pwr_data[t + '_delta']=value-pwr_data[t]
                                delta_sum += pwr_data[t + '_delta']
                            end
                            pwr_data[t]=value 
                            if value < 0
                                pwr_data[t + '_gen']= -1 * value 
                            else
                                pwr_data[t + '_gen']= 0
                            end
                            sum += value
                        end
                    end  
                    pwr_data["power_sum"] = sum     
                    pwr_data["power_sum_delta"] = delta_sum  
                    var run_avg = pwr_data.find('AvgPwr_Active')
                    if lmt != nil && lmt.contains('Sum') 
                        var s_all = lmt['Sum'].find('*')
                        var s_slot = nil
                        if limit_slot != nil
                            s_slot = lmt['Sum'].find(limit_slot)                                                        
                        end
                        var s_local_lmt = nil
                        if s_all != nil
                            s_local_lmt = s_all
                        elif s_slot != nil
                            s_local_lmt = s_slot
                        end
                        if s_local_lmt != nil
                            if  s_local_lmt.contains("max")
                                var max = s_local_lmt["max"]
                                pwr_data['Sum_o_lmt'] = max
                                if sum > max
                                    pwr_data['Sum_over']=true
                                else
                                    pwr_data['Sum_over']=false 
                                end
                                if run_avg == nil
                                    run_avg = sum
                                    pwr_data['AvgPwr_Active'] = sum
                                end
                                if run_avg != nil && run_avg > max
                                    pwr_data['AvgPwr_over']=true
                                else
                                    pwr_data['AvgPwr_over']=false 
                                end
                            end
                            if s_local_lmt.contains("max_gen")
                                var max_gen = s_local_lmt["max_gen"]
                                pwr_data['Sum_o_g_lmt'] = max_gen
                                if sum < 0 && (-1*sum) > max_gen
                                    pwr_data['Sum_over_gen']=true
                                else
                                    pwr_data['Sum_over_gen']=false 
                                end
                            end
                            if s_local_lmt.contains("min")
                                var min = s_local_lmt["min"]     
                                pwr_data['Sum_u_lmt'] = min                                                                   
                                if sum < min
                                    pwr_data['Sum_under']=true
                                else
                                    pwr_data['Sum_under']=false 
                                end
                                if run_avg != nil && run_avg < min
                                    pwr_data['AvgPwr_under']=true
                                else
                                    pwr_data['AvgPwr_under']=false 
                                end
                            end
                        end
                    end
                    var current = tasmota.millis()  
                    var time_delta = 1000000000
                    if self.data.contains(nm) && self.data[nm].contains('last')   
                        time_delta = current - self.data[nm]['last']
                    else
                        time_delta = current - tpk["subscription"]
                    end   
                    pwr_data["time_delta"] = time_delta
                    if tpk.contains('period')   
                        var per = tpk['period']                        
                        pwr_data["missing"] = (time_delta > 2*per*1000)   
                    end
                    if tpk.contains('report_delay')   
                        var report_delay = tpk['report_delay']                        
                        pwr_data["report_delay"] = report_delay                          
                    end
                    pwr_data['last'] = current  
                    self.data[nm] = pwr_data
                    try
                        self.set_consumers(nm)
                    except .. as e1,m1                        
                        self.error_push('MQTT data consumer error:' + m1)
                    end
                    ret = true                 
                end 
            end                
        except .. as e,m
            ret = false
            self.error_push('MQTT data error:' + m)
        end        
        return ret
    end

    def error_push(msg)
        if size(self.error) > 5 self.error.pop() end
        log(string.format("emQ: %s",msg))
        self.error.push(msg)
    end

    def mqtt_setup(topic, idx, data, databytes)        
        if !persist.has('emQ_set_topic') return false end       
        if !persist.emQ_set_topic return false end
        var ret = false
        if topic == persist.emQ_set_topic         
            var meters
            var actuators
            var time_slot_defs
            var limit_slot_defs
            var holidays
            try                
                var payload_json = json.load(data)               
                if payload_json != nil 
                    meters = payload_json.find('meters')                    
                    actuators = payload_json.find('actuators')                          
                    time_slot_defs = payload_json.find('time_slot_defs')  
                    limit_slot_defs = payload_json.find('limit_slot_defs') 
                    holidays = payload_json.find('holidays') 
                else
                    if data == "do_restart"
                        tasmota.cmd('Restart 1')
                    end
                end                   
                if persist.has('emQ_set')                                         
                    if meters != nil
                        log("emQ: Received meters")
                        if persist.emQ_set.contains('meters')    
                            for new_met: meters
                                var found = false
                                for i:0..size(persist.emQ_set['meters'])-1                   
                                    var met = persist.emQ_set['meters'][i]                                
                                    if new_met.contains('name') && met.contains('name') && new_met['name'] == met['name']
                                        found = true
                                        persist.emQ_set['meters'].setitem(i,new_met)
                                        self.load_meter(new_met)
                                    end
                                end
                                if !found
                                    persist.emQ_set['meters'].push(new_met)
                                    self.load_meter(new_met)
                                end
                            end
                        else
                            persist.emQ_set['meters'] = list()
                            for new_met: meters
                                persist.emQ_set['meters'].push(new_met)                                
                                self.load_meter(new_met)
                            end 
                        end
                    end
                    if actuators != nil
                        log("emQ: Received actuators")
                        if persist.emQ_set.contains('actuators')  
                            for new_act: actuators
                                var found = false
                                for i:0..size(persist.emQ_set['actuators'])-1                       
                                    var act = persist.emQ_set['actuators'][i]                                
                                    if new_act.contains('name') && act.contains('name') && new_act['name'] == act['name']
                                        found = true                                        
                                        persist.emQ_set['actuators'].setitem(i,new_act)
                                        self.load_actuator(new_act)
                                    end
                                end
                                if !found
                                    persist.emQ_set['actuators'].push(new_act)
                                    self.load_actuator(new_act)
                                end
                            end  
                        else
                            persist.emQ_set['actuators'] = list()
                            for new_act: actuators
                                persist.emQ_set['actuators'].push(new_act)  
                                self.load_actuator(new_act)                              
                            end 
                        end                        
                    end
                    if time_slot_defs != nil
                        log("emQ: Received time slot definitions")
                        persist.emQ_set['time_slot_defs'] = time_slot_defs 
                    end
                    if limit_slot_defs != nil
                        log("emQ: Received limit slot definitions")
                        persist.emQ_set['limit_slot_defs'] = limit_slot_defs 
                    end
                    if holidays != nil
                        log("emQ: Received holidays")
                        persist.emQ_set['holidays'] = holidays 
                    end
         
                    # save to _persist.json
                    persist.save()                    
                end
                ret = true
            except .. as e,m                
                self.error_push('MQTT setup error:' + m)
            end
        end  
        return ret      
    end
  
end

electricity_monitor_MQTT.driver = ElectricityMonitorMQTT
electricity_monitor_MQTT.setup_command = electricity_monitor_MQTT_setup
electricity_monitor_MQTT.setup_command_name = 'ElectricityMonitorMQTTSetup'

return electricity_monitor_MQTT