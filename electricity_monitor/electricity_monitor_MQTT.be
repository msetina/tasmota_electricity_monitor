#-
 - 
 -#
 import json
 import persist
 import mqtt
 import string 
 import backoff_actuator


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

    def prep_actuators()
        if persist.has('emQ_set') && persist.emQ_set.contains('actuators')            
            var generator_cnt = 0
            var consumer_cnt = 0
            var actuator_cnt = size(persist.emQ_set['actuators'])
            for act: persist.emQ_set['actuators']                
                if act.contains('name')
                    var nm = act['name']
                    var run_id
                    var backoff_id
                    var alloff_id
                    if act.contains('run_name')
                        var run_nm = act['run_name']
                        if self.relay_idents.contains(run_nm)
                            run_id = self.relay_idents[run_nm]
                        end
                    end
                    if act.contains('backoff_name')
                        var backoff_nm = act['backoff_name']
                        if self.relay_idents.contains(backoff_nm)
                            backoff_id = self.relay_idents[backoff_nm]
                        end
                    end
                    if self.relay_idents.contains('All off')
                        alloff_id = self.relay_idents['All off']
                    end                    
                    if act.contains('type')
                        if act['type'] == 'generator'
                            generator_cnt += 1  
                        elif act['type'] == 'consumer'  
                            consumer_cnt += 1                                                            
                        end
                    end     
                                   
                    self.actuators[nm] = backoff_actuator.actuator(act,alloff_id)                                    
                    actuator_cnt -= 1
                end
            end
        end
    end

    def prep_topics()
        if persist.has('emQ_set') && persist.emQ_set.contains('meters') 
            for met: persist.emQ_set['meters'] 
                if met.contains('name')  
                    var nm = met['name'] 
                    var value_keys = nil             
                    var limits = nil 
                    var energy_keys = nil
                    if met.contains('value_keys')
                        value_keys = met['value_keys']  
                    end
                    if met.contains('energy_keys')
                        energy_keys = met['energy_keys']  
                    end
                    if met.contains('limits')
                        limits = met['limits']  
                    end
                    var publish_period = nil              
                    if met.contains('publish_period')
                        publish_period = met['publish_period']
                    end
                    var report_delay = nil
                    if met.contains('report_delay')
                        report_delay = met['report_delay']                        
                    end
                    if met.contains('topic')
                        var topic = met['topic']
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
                        self.topics[topic]["name"] = nm
                        self.topics[topic]["subscription"] = tasmota.millis()
                        mqtt.subscribe(topic)
                        log('emQ: Subscribed ' + topic)            
                    end                    
                end
            end
        end
    end

    def get_time_slot()
        var timer_tp_translation = {'h':'hour','m':'month','dow':'weekday'}
        var ret = map()
        var k = tasmota.rtc('local')
        var current_time = tasmota.time_dump(k)
        if persist.has('emQ_set')
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
                        for time_tp: time_slot.keys()
                            if time_slot_i.contains(time_tp)
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

    def set_generators()                      
        if persist.has('emQ_set')
            for k :self.actuators.keys()
                var act = self.actuators[k]
                act.control_actuator(self.data)                
            end
        end
    end

    def set_consumers(name)                      
        if persist.has('emQ_set')            
            for k :self.actuators.keys()
                var act = self.actuators[k]
                if act.control_id == name
                    if self.data != nil
                        act.control_actuator(self.data)                
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
                    msg += string.format('{s}%s %s{m}%s{e}',k,nm,vm[nm]) 
                end
                continue
            elif k=='limit_slot'
                var vm = self.data[k]
                msg += string.format('{s}%s{m}%s{e}',k,vm['slot'])                 
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
    # def json_append()
    #     if !self.data return nil end  #- exit if not initialized -#   
    #     var msg = ''            
    #     var cnt = 0
    #     for k: self.data.keys()
    #         if k == 'Time' continue end
    #         var sens = map()
    #         sens['Power'] = map()                        
    #         for sk: self.data[k].keys()                              
    #             if sk == 'Target energy'
    #                 sens['Power']['Total'] = self.data[k][sk]/1000
    #             elif sk =='Energy for output'
    #                 sens['Power']['ForOutput'] = self.data[k][sk]/1000
    #             else
    #                 sens[sk] = self.data[k][sk]
    #             end                
    #         end            
    #     end                  
    #     tasmota.response_append(msg)
    # end

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
                    var energy_window = 900000
                    var hour_to_ms = 3600000
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
                        limit_slot = limit_slot_data['slot']
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
                        if size(self.error) > 5 self.error.pop() end
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
            try                
                var payload_json = json.load(data)               
                if payload_json != nil 
                    meters = payload_json.find('meters')                    
                    actuators = payload_json.find('actuators')                          
                    time_slot_defs = payload_json.find('time_slot_defs')  
                    limit_slot_defs = payload_json.find('limit_slot_defs') 
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
                                    end
                                end
                                if !found
                                    persist.emQ_set['meters'].push(new_met)
                                end
                            end
                        else
                            persist.emQ_set['meters'] = list()
                            for new_met: meters
                                persist.emQ_set['meters'].push(new_met)                                
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
                                    end
                                end
                                if !found
                                    persist.emQ_set['actuators'].push(new_act)
                                end
                            end  
                        else
                            persist.emQ_set['actuators'] = list()
                            for new_act: actuators
                                persist.emQ_set['actuators'].push(new_act)                                
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