#pragma once
#include "RTree.h"

// typedef rangedelete_rep::RTree<bool, uint64_t, 2, float, 4> RTreeType;
// typedef rangedelete_rep::RTree<bool, uint64_t, 2, float, 8> RTreeType;
// typedef rangedelete_rep::RTree<uint64_t, uint64_t, 2, float, 16> RTreeType;
typedef rangedelete_rep::RTree<uint32_t, uint64_t, 2, float, 16> RTreeType;
// typedef rangedelete_rep::RTree<bool, uint64_t, 2, float, 64> RTreeType;
// typedef rangedelete_rep::RTree<bool, uint64_t, 2, float, 256> RTreeType;

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

  bool update(uint64_t &x_in, uint64_t &y_in){
    if (x == x_in && y == y_in){
      return false;
    }
    x = x_in;
    y = y_in;
    return true;
  }

  bool update(const uint64_t other[2]){
    if (x == other[0] && y == other[1]){
      return false;
    }
    x = other[0];
    y = other[1];
    return true;
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

  // return whether overlaps with the rectangle determined by the two points in X axis
  bool OverlapInX(Point point_min, Point point_max) const{
    bool result = (max.x < point_min.x || min.x > point_max.x);
    return !result;
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