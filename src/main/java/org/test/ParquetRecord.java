package org.test;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Set;

@Getter
@AllArgsConstructor
public class ParquetRecord {
    String attributeName;
    byte[] hllBytes;
    byte[] hllHexBytes;
}
