package org.fbase.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class Person {
  int id;
  String firstname;
  String lastname;
  int house;
  String city;
  LocalDateTime birthday = LocalDateTime.of(2023, 1, 1, 1, 1, 1);
}
