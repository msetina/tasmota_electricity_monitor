import string
import mqtt
import json

var backoff_actuator = module('backoff_actuator')

class BackoffActuator   
    var name 
    var relays
    var alloff_id       
    var control_id
    var control_value
    var max_on_time
    var min_off_time
    var min_on_time     
    var off_milis
    var on_milis
    var prev_state    
    var mqtt_state_check
    var is_on
    var power    
    var last_on_time
    var last_off_time


    def init(settings,alloff_id)    
        self.is_on = false       
        self.last_off_time = nil
        self.last_on_time = nil      
        if settings.contains('name')   
            self.name = settings['name']
        else
            self.name = string.format('BackoffActuator')
        end
        if settings.contains('relays')                 
            self.relays = settings['relays']
            for ident: self.relays.keys()
                var rl = self.relays[ident]
                var nm = self.name
                if rl.contains('name')
                    nm = rl['name']
                end
                if type(ident)=='int'               
                    if ident < 8
                        var cmnd = string.format('FriendlyName%s %s',ident+1,nm)
                        tasmota.cmd(cmnd)
                    end
                    var cmnd2 = string.format('WebButton%s %s',ident+1,nm)
                    tasmota.cmd(cmnd2)
                end
                if type(ident)=='str'
                    if self.relays.contains(ident)                                                                              
                        self.relays[ident]["subscription"] = tasmota.millis()
                        mqtt.subscribe(ident)
                    end
                end
            end
        else
            self.relays = map()
        end
        if settings.contains('control_id')     
            self.control_id = settings['control_id']
        end
        if settings.contains('control_value')     
            self.control_value = settings['control_value']
        end        
        if settings.contains('power')     
            self.power = settings['power']
        end
        if settings.contains('mqtt_state_check')     
            self.mqtt_state_check = settings['mqtt_state_check']
            mqtt.subscribe(self.mqtt_state_check)
        end        
        if settings.contains('max_on_time')     
            self.max_on_time = settings['max_on_time']
        end
        if settings.contains('min_on_time')     
            self.min_on_time = settings['min_on_time']
        end
        if settings.contains('min_off_time')     
            self.min_off_time = settings['min_off_time']
            self.off_milis = -1000 * self.min_off_time
        else
            self.off_milis = 0
        end        
        self.alloff_id = alloff_id
    end

    def get_active_power()
        if self.is_on
            return self.power
        end
        return 0
    end

    def turn_off()
        log('emQ: Turning off')
        var outputs = tasmota.get_power()  
        for id: self.relays.keys()
            if type(id)=='string'      
                var cmnd = id
                if self.relays[id].contains('cmnd')
                    cmnd = self.relays[id]['cmnd']
                end
                mqtt.publish(cmnd,'OFF')
                log('emQ: Turned off ' + cmnd)
            elif type(id)=='int'
                if size(outputs)>=id && outputs[id]
                    tasmota.set_power(id,false)
                end
            end
        end                
        self.off_milis = tasmota.millis()  
        self.on_milis = nil      
    end

    def turn_on()          
        log('emQ: Turning on')
        var outputs = tasmota.get_power()            
        for id: self.relays.keys()
            if type(id)=='string'  
                var cmnd = id
                if self.relays[id].contains('cmnd')
                    cmnd = self.relays[id]['cmnd']
                end               
                mqtt.publish(cmnd,'ON')
                log('emQ: Turned on ' + cmnd)
            elif type(id)=='int'                
                if size(outputs)>=id && outputs[id]
                    tasmota.set_power(id,true)
                end
            end
        end                
        self.on_milis = tasmota.millis()  
        self.off_milis = nil        
    end

    def control_actuator(data,future)
        var ret = 0
        var new_state = self.calc_new_state(data,future)
        if self.prev_state != new_state
            if new_state
                self.turn_on()
                ret = self.power
            else
                self.turn_off()
            end
            self.prev_state = new_state
        end
        return ret
    end

    def check_can_turn_on()
        #log('emQ: Checking if can turn on')
        if self.off_milis != nil && self.min_off_time != nil
            return ((tasmota.millis() - self.off_milis) > self.min_off_time*1000)
        else
            return true
        end
    end

    def check_can_turn_off()
        #log('emQ: Checking if can turn off')
        if self.on_milis != nil && self.min_on_time != nil
            return ((tasmota.millis() - self.on_milis) > self.min_on_time*1000)
        else
            return true
        end
    end

    def check_needs_turn_off()
        #log('emQ: Checking if needs to turn off')
        if self.on_milis != nil && self.max_on_time != nil
            return ((tasmota.millis() - self.on_milis) > self.max_on_time*1000)
        else
            return false
        end
    end

    def calc_new_state(data,future)        
        #log('emQ: Calculating new state')        
        var outputs = tasmota.get_power()        
        if self.check_needs_turn_off()                            
            return false
        end  
        if self.alloff_id != nil && (size(outputs)>=self.alloff_id) && outputs[self.alloff_id]
            return false
        end     
        #log('emQ: Using control ID')
        if self.control_id != nil
            var ct_id = data.find(self.control_id)            
            if self.control_value != nil && ct_id != nil
                var report_delay = 0        
                if ct_id.contains('report_delay') && ct_id['report_delay'] != nil                    
                    report_delay = ct_id['report_delay'] * 1000                    
                end
                var cv = ct_id.find(self.control_value)
                var f_cv = future.find(self.control_value)                
                var ap_cv = ct_id.find('AvgPwr_Active')
                var s_cv = ct_id.find('power_sum')
                if ap_cv != nil
                    s_cv = ap_cv
                end
                if cv != nil
                    #log('emQ: Got control value')
                    if !self.is_on
                        #log(string.format('emQ: Remote is not on %d (%d) + %d',cv,s_cv,self.power))  
                        if f_cv != nil
                            cv += f_cv
                            if s_cv != nil
                                s_cv += f_cv
                            end
                        end                      
                        if self.last_off_time != nil
                            if (tasmota.millis() - self.last_off_time) > report_delay
                                cv = cv + self.power
                                if s_cv != nil
                                    s_cv = s_cv + self.power
                                end
                            end
                        else
                            cv = cv + self.power
                            if s_cv != nil
                                s_cv = s_cv + self.power
                            end
                        end
                        #log(string.format('emQ: Control value is %d. Sum %d',cv,s_cv))
                    end
                    var max_value_ident = self.control_value + '_o_lmt'
                    var min_value_ident = self.control_value + '_u_lmt'
                    var max_value = ct_id.find(max_value_ident)
                    var min_value = ct_id.find(min_value_ident)
                    var s_max_value = ct_id.find('Sum_o_lmt')
                    var s_min_value = ct_id.find('Sum_u_lmt')
                    if max_value == nil
                        max_value = 10000000
                    end  
                    if min_value == nil
                        min_value = -10000000
                    end    
                    if s_max_value == nil
                        s_max_value = 10000000
                    end  
                    if s_min_value == nil
                        s_min_value = -10000000
                    end              
                    #log('emQ: Checking control value ')  
                    if s_max_value != nil
                        var ret = self.check_turn_on_w_sum(cv,max_value,s_cv,s_max_value)
                        if ret != nil
                            return ret
                        end
                    else
                        var ret = self.check_turn_on(cv,max_value)
                        if ret != nil
                            return ret
                        end
                    end
                end
            end
        else
            return false
        end
        return self.prev_state
    end

    def check_turn_on(cv,max_value)
        if (cv >= max_value ) && self.check_can_turn_on()                        
            log(string.format('emQ: (actuator %s) Control value is bigger than its limit %d',self.name,max_value)) 
            return true
        end
        if cv < max_value 
            if self.check_can_turn_off()                                                     
                return false
            end
        end
        return nil
    end

    def check_turn_on_w_sum(cv,max_value,s_cv,s_max_value)
        if (cv >= max_value || s_cv >= s_max_value) && self.check_can_turn_on()    
            if cv >= max_value
                log(string.format('emQ: (actuator %s) Control value is bigger than its limit %d',self.name,max_value))  
            end  
            if s_cv >= s_max_value
                log(string.format('emQ: (actuator %s) Cumulative control value is bigger than its limit %d',self.name,s_max_value))  
            end                    
            return true
        end
        if cv < max_value && s_cv < s_max_value
            if self.check_can_turn_off()                                                     
                return false
            end
        end
        return nil
    end

    def mqtt_data(topic, idx, data, databytes)
        var ret = false                                        
        try           
                if self.relays.contains(topic)
                    var rl = self.relays[topic] 
                    var payload_json = json.load(data)
                    if payload_json != nil 
                    else                        
                        if data == 'ON'
                            rl['is_on'] = true
                            rl['last_on_time'] = tasmota.millis()
                        elif data == 'OFF'
                            rl['is_on'] = false
                            rl['last_off_time'] = tasmota.millis()
                        end                                            
                    end
                    ret = true 
                end
                if self.mqtt_state_check == topic                                    
                    var payload_json = json.load(data)
                    if payload_json != nil                         

                    else                        
                        if data == 'ON'
                            log(string.format('emQ: (actuator %s) Remote is on',self.name))
                            self.is_on = true
                            self.last_on_time = tasmota.millis()
                        elif data == 'OFF'
                            log(string.format('emQ: (actuator %s) Remote is off',self.name))
                            self.is_on = false
                            self.last_off_time = tasmota.millis()
                        end
                    end
                    ret = true
                end            
        except .. as e,m
            ret = false
            self.error.push('Actuator remotes MQTT data error:' + m)
        end        
        return ret
    end

end


backoff_actuator.actuator = BackoffActuator
return backoff_actuator