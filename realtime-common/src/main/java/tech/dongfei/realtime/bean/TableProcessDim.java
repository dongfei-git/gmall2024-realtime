package tech.dongfei.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcessDim {

    String sourceTable;
    String sinkTable;
    String sinkColumns;
    String sinkFamily;
    String sinkRowKey;
    String op;

}
