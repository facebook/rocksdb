#pragma once

namespace rangedelete_rep{

struct Point {
  Point() {}

  Point(Point &other) {
    x = other.x;
    y = other.y;
  }

  Point(uint64_t x_in, uint64_t y_in) {
    x = x_in;
    y = y_in;
  }

  void init_min(){
    x = std::numeric_limits<uint64_t>::max();
    y = std::numeric_limits<uint64_t>::max();
  }

  void init_max(){
    x = 0;
    y = 0;
  }

  void set_max(const Point & b){
    if (b.x > x){
      x = b.x;
    }
    if (b.y > y){
      y = b.y;
    }
  }

  void set_min(const Point & b){
    if (b.x < x){
      x = b.x;
    }
    if (b.y < y){
      y = b.y;
    }
  }

  bool operator==(const Point &other) const {
    return x == other.x && y == other.y;
  }

  bool operator<(const Point &other) const {
    return x < other.x || y < other.y;
  }

  bool operator>(const Point &other) const {
    return x > other.x || y > other.y;
  }

  uint64_t x;
  uint64_t y;
};

struct Rectangle {
  Rectangle() {}

  Rectangle(uint64_t min_x, uint64_t min_y, uint64_t max_x, uint64_t max_y) {
    min.x = min_x;
    min.y = min_y;
    max.x = max_x;
    max.y = max_y;
  }

  bool operator==(const Rectangle &other) const {
    return min == other.min && max == other.max;
  }

  Point min;
  Point max;
};

struct RectForRTree {
  RectForRTree() {}

  RectForRTree(const Rectangle & rect) {
    min[0] = rect.min.x;
    min[1] = rect.min.y;
    max[0] = rect.max.x;
    max[1] = rect.max.y;
  }

  RectForRTree(uint64_t min_x, uint64_t min_y, uint64_t max_x, uint64_t max_y) {
    min[0] = min_x;
    min[1] = min_y;
    max[0] = max_x;
    max[1] = max_y;
  }

  bool operator==(const RectForRTree &other) const {
    return min[0] == other.min[0] && min[1] == other.min[1]
            && max[0] == other.max[0] && max[1] == other.max[1];
  }

  uint64_t min[2];
  uint64_t max[2];
};

}