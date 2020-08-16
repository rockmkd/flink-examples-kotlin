package kr.rockmkd.common.sources.builder

interface ElementBuilder<T> {

  fun newElement(): T

}
