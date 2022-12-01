package org.fbase.model;


import static com.querydsl.core.types.PathMetadataFactory.forVariable;

import com.querydsl.core.types.Path;
import com.querydsl.core.types.PathMetadata;
import com.querydsl.core.types.dsl.EntityPathBase;
import com.querydsl.core.types.dsl.NumberPath;
import com.querydsl.core.types.dsl.StringPath;
import javax.annotation.processing.Generated;

@Generated("com.querydsl.codegen.EntitySerializer")
public class QPerson extends EntityPathBase<Person> {
  private static final long serialVersionUID = -479242270L;

  public static final QPerson person = new QPerson("person");

  public final NumberPath<Integer> id = createNumber("id", Integer.class);

  public final StringPath firstname = createString("firstname");
  public final StringPath lastname = createString("lastname");
  public final NumberPath<Integer> house = createNumber("house", Integer.class);
  public final StringPath city = createString("city");

  public QPerson(String variable) {
    super(Person.class, forVariable(variable));
  }

  public QPerson(Path<? extends Person> path) {
    super(path.getType(), path.getMetadata());
  }

  public QPerson(PathMetadata metadata) {
    super(Person.class, metadata);
  }

}
