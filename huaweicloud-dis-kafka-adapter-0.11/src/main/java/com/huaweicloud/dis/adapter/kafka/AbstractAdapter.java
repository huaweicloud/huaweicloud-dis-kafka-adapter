/*
 * Copyright 2002-2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huaweicloud.dis.adapter.kafka;

import java.util.Map;
import java.util.Properties;

import com.huaweicloud.dis.DISClient;
import com.huaweicloud.dis.DISConfig;


public abstract class AbstractAdapter
{
    
    protected DISClient disClient;
   
    protected DISConfig config;
    
    public AbstractAdapter(){
    }
    
    public AbstractAdapter(Map map){
        DISConfig disConfig = new DISConfig();
        disConfig.putAll(map);
        
        this.config = disConfig;
        this.disClient = new DISClient(disConfig);
    }
    
    public AbstractAdapter(Properties properties){
        this((Map)properties);
    }
    
    public AbstractAdapter(DISConfig disConfig){
        this.config = disConfig;
        this.disClient = new DISClient(disConfig);
    }
    
    
}
