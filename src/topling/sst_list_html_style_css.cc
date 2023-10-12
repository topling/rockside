#include <string_view>

namespace rocksdb {

extern const std::string_view g_sst_list_html_style_css =
R"EOS(<style>
  div * td {
    text-align: right;
    font-family: monospace;
  }
  div * td.left {
    text-align: left;
    font-family: Sans-serif;
  }
  div * td.monoleft {
    text-align: left;
    font-family: monospace;
    max-width: 40ch;
    word-break: break-all;
  }
  div * td.center {
    text-align: center;
    font-family: Sans-serif;
  }

  .emoji {
    font-family: "Apple Color Emoji","Segoe UI Emoji",NotoColorEmoji,"Segoe UI Symbol","Android Emoji",EmojiSymbols;
  }
  em {
    color: brown;
  }

  .bghighlight {
    background-color: AntiqueWhite;
  }

  .highlight00 {
    background-color: AntiqueWhite; color: Black;
  }
  .highlight01 {
    background-color: AntiqueWhite; color: Teal;
  }
  .highlight02 {
    background-color: AntiqueWhite; color: Fuchsia;
  }
  .highlight03 {
    background-color: AntiqueWhite; color: Purple;
  }

  .highlight04 {
    background-color: Pink; color: Black;
  }
  .highlight05 {
    background-color: Pink; color: Teal;
  }
  .highlight06 {
    background-color: Pink; color: Purple;
  }
  .highlight07 {
    background-color: Pink; color: Navy;
  }

  .highlight08 {
    background-color: DarkCyan; color: White;
  }
  .highlight09 {
    background-color: DarkCyan; color: Orange;
  }
  .highlight10 {
    background-color: DarkCyan; color: Pink;
  }
  .highlight11 {
    background-color: DarkCyan; color: Yellow;
  }

  .highlight12 {
    background-color: Green; color: White;
  }
  .highlight13 {
    background-color: Green; color: Orange;
  }
  .highlight14 {
    background-color: Green; color: Pink;
  }
  .highlight15 {
    background-color: Green; color: Yellow;
  }

  .highlight16 {
    background-color: Indigo; color: White;
  }
  .highlight17 {
    background-color: Indigo; color: Orange;
  }
  .highlight18 {
    background-color: Indigo; color: Pink;
  }
  .highlight19 {
    background-color: Indigo; color: Yellow;
  }

  .highlight20 {
    background-color: Navy; color: White;
  }
  .highlight21 {
    background-color: Navy; color: Orange;
  }
  .highlight22 {
    background-color: Navy; color: Pink;
  }
  .highlight23 {
    background-color: Navy; color: Yellow;
  }

  .highlight24 {
    background-color: Olive; color: White;
  }
  .highlight25 {
    background-color: Olive; color: Orange;
  }
  .highlight26 {
    background-color: Olive; color: Pink;
  }
  .highlight27 {
    background-color: Olive; color: Yellow;
  }

  .highlight28 {
    background-color: YellowGreen; color: Black;
  }
  .highlight29 {
    background-color: YellowGreen; color: Teal;
  }
  .highlight30 {
    background-color: YellowGreen; color: FireBrick;
  }
  .highlight31 {
    background-color: YellowGreen; color: Purple;
  }
</style>)EOS";

extern const unsigned int g_sst_list_html_highlight_classes = 32;

} // namespace rocksdb
