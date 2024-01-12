env {
  for_each = toset(["sqlite://foo.db?_fk=1", "sqlite://bar.db?_fk=1"])
  name     = atlas.env
  url      = each.value
  migration {
    dir = "file://migrations"
  }
}

