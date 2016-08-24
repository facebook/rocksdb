---
layout: home
title: Example - You can over-ride the default title here
id: home
---

## Gridblocks 

You can use the gridblocks sub-template to arrange information into nice responsive grids. There are two ways to add a grid block, the first is by adding a yml file to the `_data` folder and then using it as a data source:

{% include content/gridblocks.html data_source=site.data.features align="center" %}

The second is simply to use the raw HTML of the grid blocks:

<div class="gridBlock">
  <div class="blockElement twoByGridBlock alignCenter">
    <div class="blockContent">
    <h3>Feature 1</h3>
    <p>This is a description of the feature.</p>
    </div>
  </div>

  <div class="blockElement twoByGridBlock alignCenter">
    <div class="blockContent">
    <h3>Feature 2</h3>
    <p>This is another description of a feature.</p>
    </div>
  </div>
</div>

Use the first option as a preference, because it will make your site easier for non-technical folks to edit and update.

Gridblocks have additional options for layout compared to the above:

Left aligned:

{% include content/gridblocks.html data_source=site.data.features align="left" %}

Right aligned: 

{% include content/gridblocks.html data_source=site.data.features align="right" %}

Images on the side: 

{% include content/gridblocks.html data_source=site.data.features imagealign="side" %}

Four column layout:

{% include content/gridblocks.html data_source=site.data.features layout="fourColumn" align="center" %}