package com.movable.models

trait ConfigNamespace {
  def namespace: String
}

abstract class ConfigBuilder extends ConfigNamespace {
  def getConfigField(namespace: String, field: String) = s"$namespace.$field"
}

