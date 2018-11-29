/*
 * Copyright 2002-2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huaweicloud.dis.adapter.kafka;

/**
 * Created by z00382129 on 2017/10/21.
 */
public class Utils {

    static final String PARTITION_ID = "shardId";

    public static int getKafkaPartitionFromPartitionId(String partitionId){
        int zeroIndex = partitionId.indexOf("0");

        int partitionNum = Integer.parseInt(partitionId.substring(zeroIndex == -1 ? 0 : zeroIndex));
        return partitionNum;
    }

    public static String getShardIdStringFromPartitionId(int partitionId)
    {
        return String.format("%s-%010d", PARTITION_ID, partitionId);
    }


}
