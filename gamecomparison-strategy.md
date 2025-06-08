# Spiderweb Game Comparison Axes:

Each game will be scored on each axis by an assessor (or automatically via some heuristic) from 0.0 to 1.0.

**Assessor Note:** Define specific criteria for each score point, e.g., "0.0 means the player has no choice/impact; '0.5' might mean basic choices exist but don't fundamentally change outcomes; '1.0' means almost ny action or sequence of actions can lead to major consequences."

---
## List of axis

### Axis 1: Core Mechanics Complexity
*   **Definition:** How intricate and numerous are the fundamental rules and systems governing interaction within
the game? This considers the depth, uniqueness, and interconnectivity of core gameplay loops.
    *   `0.0`: Simple turn-based combat with no meaningful choices (e.g., match-3 where only one move is viable).
    *   `1.0`: Highly complex, interconnected systems like resource gathering, crafting, social interaction, character progression, advanced combat/puzzling mechanics interacting in non-linear ways (e.g., EVE Online's ship uilding and mission system).

### Axis 2: Core Mechanics Count
*   **Definition:** How many distinct core gameplay systems does the game feature? This counts the primary methods of player engagement beyond basic
survival or exploration.
    *   `0.0`: Minimalist, perhaps only one core mechanic (e.g., Snake).
    *   `1.0`: Multiple complex and integrated core mechanics (e.g., a strategy game with resource management, research, diplomacy, combat).

### Axis 3: Player Agency
*   **Definition:** The degree of freedom and perceived impact the player has on the game's state or narrative.
    *   `0.0`: Passive gameplay; choices have minimal effect (e.g., choosing which character to watch in a cutscene).
    *   `1.0`: High agency with meaningful consequences for almost any significant action, allowing players to shape their experience substantially (e.g., branching paths, major consequence systems).

### Axis 4: Player Impact
*   **Definition:** How significantly can the player's actions change the game world or its state in a long-term sense?
    *   `0.0`: Actions have negligible impact on the overall progression or outcome.
    *   `1.0`: Player actions consistently alter the core systems, environment, economy, or available paths (e.g., building an empire in Civilization).

### Axis 5: Narrative Density
*   **Definition:** The amount and depth of story elements integrated into gameplay compared to world description/pacing.
    *   `0.0`: Minimal or no narrative; purely abstract concepts or no discernible plot beyond basic tasks.
    *   `1.0`: Rich, detailed storytelling deeply woven into the player's actions (e.g., BioWare RPGs).

### Axis 6: Narrative Integration
*   **Definition:** How closely is the game's story and theme connected to its core mechanics?
    *   `0.0`: Story feels separate or tacked on; themes don't align with gameplay systems.
    *   `1.0`: Mechanics directly reinforce narrative themes, character arcs, and emotional beats (e.g., Heavy Rain).

### Axis 7: Scope / Scale
*   **Definition:** The size and complexity of the game world relative to its core mechanics count/complexity.
    *   `0.0`: Tiny scope; confined environment with limited systems. (E.g., a small puzzle).
    *   `1.0`: Massive, complex scope that feels justified given the number of systems present or mentioned in lore/story.

### Axis 8: Pacing (Controlled)
*   **Definition:** How much control does the player have over the speed of progression through the core game loop?
    *   `0.0`: Forced pacing; little choice to accelerate or decelerate gameplay significantly.
    *   `1.0`: High degree of freedom for managing time between sessions (e.g., long-term quests, optional content) and within a session.

### Axis 9: Pacing (Compelled)
*   **Definition:** How much does the game compel the player to progress at its natural pace? This includes things like increasing difficulty or narrative momentum.
    *   `0.0`: No compelling rhythm; players can easily stall indefinitely with minimal consequence.
    *   `1.0`: Clear, escalating challenges and narrative progression that strongly encourage faster play.

### Axis 10: Replayability
*   **Definition:** The extent to which the game encourages multiple distinct playthroughs due to its systems or structure.
    *   `0.0`: Single-playable; no significant factors encouraging repeat engagement (e.g., a tutorial level).
    *   `1.0`: High replay value driven by complex core mechanics, branching narratives, permadeath elements, or player-driven world changes.

### Axis 11: Player-Driven World Change
*   **Definition:** To what degree does the game allow players to meaningfully alter its state through their actions?
    *   `0.0`: The game world is static; player actions don't change it in a lasting, meaningful way.
    *   `1.0`: Players have clear tools and mechanics that let them significantly modify the environment or systems (e.g., city building).

### Axis 12: Multiplayer Integration
*   **Definition:** How tightly integrated are online multiplayer features with the core single-player experience?
    *   `0.0`: No online multiplayer, or it feels completely disconnected from the main game.
    *   `1.0`: Online play is a fundamental part of the core loop or progression system.

### Axis 13: Technical Execution (Core)
*   **Definition:** How well are the defined game mechanics implemented? Smoothness, polish, balance, responsiveness.
    *   `0.0`: Core systems feel broken, buggy, unbalanced, or unresponsive.
    *   `1.0`: Mechanics function flawlessly and cohesively.

### Axis 14: Aesthetics (Core)
*   **Definition:** How well does the game's visual style support its core mechanics? Does it enhance understanding or immersion?
    *   `0.0`: Visuals are confusing, unappealing, or detract from the gameplay.
    *   `1.0`: Visual style is highly effective in communicating mechanics and enhancing immersion.

---

## Scoring Similarity
To find similarity based on these axes:
1.  **Normalization:** Ensure all games use the same scale (e.g., a 5-point scale where reviewers calibrate their understanding of each axis).
2.  **Axis-by-Axis Overlap Calculation:**
    *   For two games, A and B, calculate the absolute difference in score for each Axis `X`:
        `delta_X = |Score_A(X) - Score_B(X)|`
    *   The overlap on a single axis is not directly calculated as an area. Instead, think of it this way: the closer the scores are (lower delta), the more similar they are *on that specific axis*. However, to get an overall similarity score based on the "spiderweb" idea:
        - Calculate for each axis `X`: `overlap_X = 1.0 - Score_A(X) / Score_B(X)`? No, this doesn't work well because scores can be close but not identical.
    *   **Alternative Approach (Common):** Calculate an average similarity score across all axes.
        `Overall_Similarity = Average( over X from 1 to N ) of [ |Score_A(X) - Score_B(X)| ]`? No, this averages the differences, which isn't intuitive for "overlap".
    *   **Better Approach (Closest to your idea):** Calculate an average similarity score across all axes.
        `Overall_Similarity = Average( over X from 1 to N ) of [ min(Score_A(X), Score_B(X)) ]`?
        *   This finds the minimum value on each axis, which gives a sense of how much they share that specific aspect (but doesn't account for games being complementary). E.g., if both have `0.5 Player Agency`, it contributes 0.5; if one has `1.0` and the other `0.0`, it contributes `0.0`. Then average all these min values.
    *   **Another Approach (Weighted Average):** You could assign weights to axes based on perceived importance, then calculate a weighted average of the individual similarity scores (`min(Score_A(X), Score_B(X))`).
        `Overall_Similarity = Sum( over X from 1 to N ) [ Weight_X * min(Score_A(X), Score_B(X)) ] / Total_Weight`
    *   **Closest Area Overlap:** This is complex because radar charts plot distance, and the area calculation isn't linear. A simpler way might be to use a Euclidean or Manhattan distance metric between points, then invert that score (so small distance = high similarity). However, this doesn't directly give an "overlap" percentage like you described.

## Recommendation
The most straightforward adaptation of your spiderweb idea is likely the **weighted average using `min(Score_A(X), Score_B(X))` on each axis**. You can define a list of axes (like above) with their definitions, then use this method to calculate an overall similarity score between 0 and 1.
You could also visualize points in multi-dimensional space (each point being the vector of scores across all axes for one game). Then calculate distances between these points or use clustering algorithms – both are powerful ways to quantify similarity beyond simple averaging.
